import { createServerFn } from "@tanstack/react-start";
import { eq } from "drizzle-orm";
import * as z from "zod/v4-mini";
import { hash } from "@/lib/password";

import { db } from "@/db";
import { accounts, users } from "@/db/schema/auth";

export const getUserByUsername = createServerFn({ method: 'GET' })
    .inputValidator(z.object({
        username: z.string(),
        password: z.string()
    }))
    .handler(async ({ data }) => {
        const [user] = await db
            .select({
                id: users.id,
                username: users.username,
                email: users.email,
                name: users.name,
                password: accounts.password
            })
            .from(users)
            .leftJoin(accounts, eq(users.id, accounts.userId))
            .where(eq(users.username, data.username))

        if (!user) {
            throw new Error('User not found')
        }

        return user
    })

export const createUser = createServerFn({ method: 'POST' })
    .inputValidator(z.object({
        username: z.string(),
        password: z.string(),
        email: z.string(),
        name: z.string()
    }))
    .handler(async ({ data }) => {
        const [user] = await db
            .insert(users)
            .values({
                username: data.username,
                email: data.email,
                name: data.name
            })
            .$returningId()

        const hashedPassword = await hash(data.password)

        await db.insert(accounts).values({
            userId: user.id,
            password: hashedPassword,
            providerId: 'credentials',
            accountId: user.id
        })

        if (!user.id) {
            throw new Error('User not found')
        }

        return user
    })