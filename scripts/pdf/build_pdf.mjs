// Erzeugt bericht/bericht.pdf aus bericht/bericht.md.
// Markdown -> HTML (marked) mit eigenem Stylesheet (bericht.css) -> PDF (Chrome via puppeteer-core).
//
// Aufruf (einmalig vorher `npm install` in scripts/pdf):
//   cd scripts/pdf && npm run pdf
// Chrome-Pfad bei Bedarf über die Umgebungsvariable CHROME_PATH setzen.

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { marked } from "marked";
import puppeteer from "puppeteer-core";

const here = dirname(fileURLToPath(import.meta.url));
const berichtDir = resolve(here, "../../bericht");
const mdPath = join(berichtDir, "bericht.md");
const htmlPath = join(berichtDir, "bericht.html");
const pdfPath = join(berichtDir, "bericht.pdf");

const chromeCandidates = [
  process.env.CHROME_PATH,
  "C:/Program Files/Google/Chrome/Application/chrome.exe",
  "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  "/usr/bin/google-chrome",
  "/usr/bin/chromium",
].filter(Boolean);
const chrome = chromeCandidates.find((p) => existsSync(p));
if (!chrome) throw new Error("Kein Chrome/Edge gefunden. Bitte CHROME_PATH setzen.");

let md = readFileSync(mdPath, "utf8");
// Arbeitshinweise fürs Team (Blockquotes mit TODO / Hinweis) nicht ins PDF übernehmen.
md = md.replace(/^> \*\*(TODO|Hinweis zur Arbeit)[^\n]*\n(>[^\n]*\n)*/gm, "");

// Überschriften erhalten IDs im GitHub-Stil (z.B. "1. Ausgangslage" -> "1-ausgangslage"),
// damit die Links im Inhaltsverzeichnis im PDF funktionieren.
const slug = (text) =>
  text.toLowerCase().trim()
    .replace(/<[^>]+>/g, "")
    .replace(/[^\p{L}\p{N}\s-]/gu, "")
    .replace(/\s/g, "-");
marked.use({
  renderer: {
    heading({ tokens, depth, text }) {
      return `<h${depth} id="${slug(text)}">${this.parser.parseInline(tokens)}</h${depth}>\n`;
    },
  },
});

const body = marked.parse(md, { gfm: true });
const css = readFileSync(join(here, "bericht.css"), "utf8");
const html = `<!doctype html>
<html lang="de-CH"><head><meta charset="utf-8"><title>Leistungsnachweis DB &amp; DWH</title>
<style>${css}</style></head><body>${body}</body></html>`;
writeFileSync(htmlPath, html, "utf8");

const browser = await puppeteer.launch({ executablePath: chrome, headless: true, args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.goto(pathToFileURL(htmlPath).href, { waitUntil: "networkidle0" });
await page.pdf({
  path: pdfPath,
  format: "A4",
  printBackground: true,
  displayHeaderFooter: true,
  headerTemplate: "<span></span>",
  footerTemplate: `<div style="width:100%;font-size:8pt;color:#666;padding:0 18mm;display:flex;justify-content:space-between;font-family:Arial,sans-serif">
    <span>Leistungsnachweis DB &amp; DWH – Erbsland, Liechti</span>
    <span>Seite <span class="pageNumber"></span> von <span class="totalPages"></span></span></div>`,
  margin: { top: "16mm", bottom: "18mm", left: "18mm", right: "18mm" },
});
await browser.close();
console.log("PDF erstellt:", pdfPath);
