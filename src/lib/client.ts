
import { AppType } from "@/db/api";
import { hc } from "hono/client";

const APP_URL = "http://localhost:3000";
// const APP_URL = process.env.NEXT_PUBLIC_APP_URL!;
export const client = hc<AppType>(APP_URL);