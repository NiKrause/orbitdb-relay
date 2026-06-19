#!/usr/bin/env node

import { readFile, writeFile } from "node:fs/promises";

const filePath = process.argv[2];

if (!filePath) {
  throw new Error("Usage: node scripts/remove-caddy-webaccess-gate.mjs <deploy-executor.ts>");
}

const source = await readFile(filePath, "utf8");
const gatedBlock = `      if (
        plan.enableCaddyProxy &&
        proxyUrl &&
        runtime.webAccess?.active !== true
      ) {
        log(
          \`[deploy] proxy URL reserved but not active yet for \${deployment.itemHash}; waiting before guest configure\`,
        );
        let latestRuntime = runtime;
        for (let attempt = 0; attempt < plan.setupAttempts; attempt += 1) {
          if (latestRuntime.webAccess?.active === true) {
            break;
          }
          await sleepImpl(plan.setupDelayMs);
          latestRuntime = await fetchVmRuntime({
            itemHash: deployment.itemHash,
            fetch: fetchImpl,
            crnHash: candidateCrn.hash,
            crns: candidateCrns,
            crnListUrl: plan.crnListUrl,
          }).catch(() => latestRuntime);
          log(
            \`[deploy] proxy activation \${attempt + 1}/\${plan.setupAttempts}: active=\${
              latestRuntime.webAccess?.active === true
            } proxy=\${latestRuntime.proxyUrl ?? "-"}\`,
          );
        }
        runtime = latestRuntime;
        runtimeMetadata.allocation = runtime.allocation;
        runtimeMetadata.hostIpv4 = runtime.hostIpv4;
        runtimeMetadata.ipv6 = runtime.ipv6;
        runtimeMetadata.proxyUrl = runtime.proxyUrl;
        runtimeMetadata.sshCommand = runtime.sshCommand;
        runtimeMetadata.mappedPorts = runtime.mappedPorts;
        runtimeMetadata.diagnostics = runtime.diagnostics;
        runtimeMetadata.selectedCrn = runtime.selectedCrn ?? {
          hash: candidateCrn.hash,
          name: candidateCrn.name ?? "",
        };
        proxyUrl = runtime.webAccess?.active === true ? (runtime.proxyUrl ?? null) : null;
        if (!proxyUrl) {
          log(
            \`[deploy] proxy URL still inactive for \${deployment.itemHash}; configuring relay without Caddy for now\`,
          );
        }
      }
`;

if (!source.includes("runtime.webAccess?.active !== true")) {
  console.log(`Caddy webAccess gate already absent in ${filePath}`);
  process.exit(0);
}

if (!source.includes(gatedBlock)) {
  throw new Error(`Could not find the expected Caddy webAccess gate block in ${filePath}`);
}

await writeFile(filePath, source.replace(gatedBlock, ""), "utf8");
console.log(`Removed the Caddy webAccess gate from ${filePath}`);
