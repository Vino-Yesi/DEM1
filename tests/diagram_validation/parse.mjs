import { JSDOM } from "jsdom";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const dom = new JSDOM("<!doctype html><html><body></body></html>", {
  pretendToBeVisual: true,
});
globalThis.window = dom.window;
globalThis.document = dom.window.document;
globalThis.navigator = dom.window.navigator;
globalThis.Element = dom.window.Element;
globalThis.SVGElement = dom.window.SVGElement;

const mermaid = (await import("mermaid")).default;
await mermaid.initialize({ startOnLoad: false, securityLevel: "loose" });

const sources = [];
const diagramsDir = path.join(root, "diagrams");
for (const name of fs.readdirSync(diagramsDir).sort()) {
  if (name.endsWith(".mmd")) {
    sources.push({
      label: `diagrams/${name}`,
      text: fs.readFileSync(path.join(diagramsDir, name), "utf8"),
    });
  }
}

function collectMarkdown(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      collectMarkdown(full);
    } else if (entry.name.endsWith(".md")) {
      const text = fs.readFileSync(full, "utf8");
      const fence = /```mermaid\s*\n([\s\S]*?)```/g;
      let match;
      let index = 0;
      while ((match = fence.exec(text)) !== null) {
        index += 1;
        sources.push({
          label: `${path.relative(root, full)}#mermaid-${index}`,
          text: match[1],
        });
      }
    }
  }
}
collectMarkdown(root);

let ok = 0;
for (const source of sources) {
  try {
    const result = await mermaid.parse(source.text);
    console.log(`PARSE OK ${source.label} ${result.diagramType}`);
    ok += 1;
  } catch (error) {
    console.error(`PARSE FAILED ${source.label}: ${error.message || error}`);
  }
}
console.log(`PARSED ${ok}/${sources.length} Mermaid sources`);
if (ok !== sources.length) process.exitCode = 1;
