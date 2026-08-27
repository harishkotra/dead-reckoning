// web/src/main.tsx
//
// Entry point for the Dead Reckoning web UI. Mounts the App into the DOM and
// imports the raw-CSS stylesheet. No UI framework — plain React + CSS only.

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles.css";

const rootElement = document.getElementById("root");
if (!rootElement) {
  throw new Error("Root element #root not found");
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
