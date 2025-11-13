
import { AppType } from "@/db/api";
import { createIsomorphicFn } from "@tanstack/react-start";
import { hc } from "hono/client";

const APP_URL = import.meta.env.VITE_APP_URL!;

export const client = hc<AppType>(APP_URL)
export const getIsomorphicClient = createIsomorphicFn()
    .server(() => hc<AppType>(APP_URL, { fetch: globalThis.fetch }).api)
    .client(() => hc<AppType>(APP_URL).api)