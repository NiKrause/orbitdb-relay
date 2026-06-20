#!/usr/bin/env node

import { readFile, writeFile } from "node:fs/promises";

const filePath = process.argv[2];

if (!filePath) {
  throw new Error("Usage: node scripts/remove-caddy-webaccess-gate.mjs <deploy-executor.ts>");
}

const source = await readFile(filePath, "utf8");
if (!source.includes("runtime.webAccess?.active !== true")) {
  console.log(`Caddy webAccess gate already absent in ${filePath}`);
  process.exit(0);
}

const startAnchor = `      if (
        plan.enableCaddyProxy &&
        proxyUrl &&
        runtime.webAccess?.active !== true`;
const endAnchor = `

      if (setupPort && runtime.hostIpv4) {`;
const start = source.indexOf(startAnchor);
const end = source.indexOf(endAnchor, start);

if (start < 0 || end < 0) {
  throw new Error(`Could not find the expected Caddy webAccess gate block in ${filePath}`);
}

await writeFile(filePath, `${source.slice(0, start)}${source.slice(end + 1)}`, "utf8");
console.log(`Removed the Caddy webAccess gate from ${filePath}`);
