import { createServerFn } from "@tanstack/react-start";
import * as z from "zod/v4-mini";

export const createUser = createServerFn({ method: 'POST' })
    .inputValidator(z.object({
        username: z.string(),
        password: z.string(),
        email: z.string(),
        name: z.string()
    }))
    .handler(async ({ data }) => {
        // Dynamically import server-only logic to avoid top-level bundle inclusion
        const { createUserInDb } = await import("./user.server");

        try {
            const user = await createUserInDb(data);
            return user;
        } catch (error) {
            console.error("Failed to create user", error);
            // Re-throw or handle error appropriately to propagate to client
            throw error;
        }
    })