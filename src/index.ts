import cors from "cors";
import express from "express";
import { sql } from "./db.js";
import { env } from "./env.js";

const app = express();

app.use(cors());
app.use(express.json());

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.get("/health/db", async (_req, res) => {
  try {
    const rows = await sql<{ now: Date }[]>`select now()`;
    res.json({ status: "ok", now: rows[0]?.now });
  } catch (err) {
    res.status(500).json({ status: "error", error: (err as Error).message });
  }
});

app.listen(env.port, () => {
  console.log(`API listening on http://localhost:${env.port}`);
});
