/**
 * Access controllers an operator adds to the relay by configuration.
 *
 * The relay opens a database from its manifest and checks every entry with the
 * access controller the manifest names, so it can only replicate databases whose
 * controller it has registered. Apart from the built-in ones, a relay can be
 * given more:
 *
 * - `RELAY_ACCESS_CONTROLLERS`: comma-separated module specifiers. A path
 *   (`./x.js`, `../x.js`, `/abs/x.js`, `file:` URL) is resolved against the
 *   working directory; a bare package name is resolved from where orbitdb-relay
 *   is installed, so the package has to be installed next to it.
 * - `startRelay({ accessControllers })` or
 *   `orbitdbReplicationService({ accessControllers })` when used as a library.
 *
 * A module provides its controllers as its default export (one controller or an
 * array) or as a named `accessControllers` array. A controller is what OrbitDB's
 * `useAccessController` takes: a factory function with a string `type`.
 *
 * Only code that is already on the machine is loaded; there is no way to fetch a
 * controller from the network. A configured type may not replace one the relay
 * brings itself.
 */
import { isAbsolute, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { useAccessController } from "@orbitdb/core";

/** An OrbitDB access controller factory, as `useAccessController` takes it. */
export type AccessControllerModule = ((...args: any[]) => any) & { type: string };

export const ACCESS_CONTROLLERS_ENV = "RELAY_ACCESS_CONTROLLERS";

/** Types the relay registers itself (plus core's built-in `orbitdb`); configuration cannot replace them. */
export const BUILT_IN_ACCESS_CONTROLLER_TYPES: ReadonlySet<string> = new Set([
  "ipfs",
  "orbitdb",
  "orbitdb-deferred",
  "todo-delegation",
]);

/** `"a, ./b.js,,c"` → `["a", "./b.js", "c"]` */
export function parseAccessControllerSpecifiers(value: string | undefined): string[] {
  return (value ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

/** A path becomes a file URL against `cwd`; a package name stays as it is. */
export function toImportSpecifier(specifier: string, cwd: string = process.cwd()): string {
  if (specifier.startsWith("file:")) return specifier;
  if (specifier.startsWith("./") || specifier.startsWith("../") || isAbsolute(specifier)) {
    return pathToFileURL(resolve(cwd, specifier)).href;
  }
  return specifier;
}

export function isAccessControllerModule(value: unknown): value is AccessControllerModule {
  return typeof value === "function" && typeof (value as any).type === "string" && (value as any).type !== "";
}

/** The controllers a module exports: its default export (one or an array) and a named `accessControllers` array. */
export function accessControllersFromModule(mod: any): AccessControllerModule[] {
  const candidates: unknown[] = [];
  const add = (value: unknown) => {
    if (Array.isArray(value)) candidates.push(...value);
    else if (value != null) candidates.push(value);
  };
  add(mod?.default);
  add(mod?.accessControllers);
  return [...new Set(candidates.filter(isAccessControllerModule))];
}

/**
 * Import every module and collect its controllers. Throws when a module can't be
 * imported or exports none: a relay that silently lacks a configured controller
 * would refuse that app's databases later, with nothing pointing at the cause.
 */
export async function loadAccessControllers(
  specifiers: string[],
  {
    cwd = process.cwd(),
    importModule = (specifier: string) => import(specifier),
  }: { cwd?: string; importModule?: (specifier: string) => Promise<any> } = {},
): Promise<AccessControllerModule[]> {
  const loaded: AccessControllerModule[] = [];
  for (const specifier of specifiers) {
    let mod: any;
    try {
      mod = await importModule(toImportSpecifier(specifier, cwd));
    } catch (error) {
      throw new Error(
        `${ACCESS_CONTROLLERS_ENV}: cannot import "${specifier}": ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    const controllers = accessControllersFromModule(mod);
    if (controllers.length === 0) {
      throw new Error(
        `${ACCESS_CONTROLLERS_ENV}: "${specifier}" exports no access controller (a function with a string "type", as default export or in "accessControllers")`,
      );
    }
    loaded.push(...controllers);
  }
  return loaded;
}

/** The controllers named in `RELAY_ACCESS_CONTROLLERS`, if any. */
export async function loadAccessControllersFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): Promise<AccessControllerModule[]> {
  return loadAccessControllers(parseAccessControllerSpecifiers(env[ACCESS_CONTROLLERS_ENV]));
}

/**
 * Register additional controllers with OrbitDB. Refuses a type the relay brings
 * itself, so configuration can add controllers but never change how the relay
 * checks the databases it already understands.
 *
 * @returns the registered types
 */
export function registerAccessControllers(
  controllers: AccessControllerModule[] = [],
  use: (controller: AccessControllerModule) => void = useAccessController as any,
): string[] {
  const types: string[] = [];
  for (const controller of controllers) {
    if (!isAccessControllerModule(controller)) {
      throw new Error("not an access controller: expected a function with a string \"type\"");
    }
    if (BUILT_IN_ACCESS_CONTROLLER_TYPES.has(controller.type)) {
      throw new Error(`access controller type "${controller.type}" is built into the relay and cannot be replaced`);
    }
    use(controller);
    types.push(controller.type);
  }
  return types;
}
