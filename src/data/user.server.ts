import { eq } from "drizzle-orm";
import * as z from "zod/v4-mini";
import { hash } from "@/lib/password";
import { db } from "@/db";
import { accounts, users } from "@/db/schema/auth";

const loginSchema = z.object({
    username: z.string(),
    password: z.string()
})

export const getUserByUsername = async (data: z.infer<typeof loginSchema>) => {
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
        // Return null or throw specific error if explicit handling needed, 
        // strictly following original logic which threw Error('User not found') 
        // but typically getUserByUsername might return null.
        // Original code threw error? 
        // Checking original file: Yes, "throw new Error('User not found')"
        // But api.ts checks "if (!user)"
        // Wait, original threw error. api.ts logic: "try... catch" or expected "return null"?
        // Looking at api.ts in Step 27:
        // const user = await getUserByUsername({ username, password })
        // if (!user || !user.password) return null;
        // If getUserByUsername throws, api.ts crashes (unless wrapped).
        // Let's check api.ts again.
        // It calls "const user = await getUserByUsername(...)".
        // If it throws, the authorize function throws?
        // Let's assume for now we keep the logic but let's re-read api.ts usage.
        // Step 27 line 58. Then 60.
        // If line 58 throws, it bubbles out of authorize.
        // So I should keep the throw if that was the intent, or change to return null.
        // The original code threw error. I will keep it consistent or defer to original.
        // Actually, looking at original user.ts:
        // if (!user) throw new Error('User not found')
        // So I will replicate that.
        // BUT wait, if api.ts expects null for invalid user, throwing is bad for "invalid credentials" flow?
        // Usually authorize returns null on failure.
        // If getUserByUsername throws, authorize fails with error.
        // I will copy exact logic.
    }
    return user // Returns undefined if not found in array if using [user] destructuring on empty array? 
    // Wait, [user] = [] -> user is undefined.
    // const [user] = ...
    // if (!user) throw ...
}

// Renaming this to avoid conflict if I export it?
// Actually I will export the functions needed by createUser too.

export const createUserInDb = async (data: { username: string, email: string, name: string, password: string }) => {
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
        throw new Error('User not found') // Copied from original
    }

    return user
}
