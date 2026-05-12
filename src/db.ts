import postgres from "postgres";
import { env } from "./env.js";

export const sql = postgres(env.databaseUrl, {
  prepare: false,
  ssl: env.nodeEnv === "production" ? "require" : undefined,
});
