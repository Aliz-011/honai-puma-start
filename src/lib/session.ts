import { Session } from '@auth/core/types';
import { createServerFn } from '@tanstack/react-start';
import { useSession } from '@tanstack/react-start/server'

type SessionData = {
    id: string;
    name?: string;
    email?: string
    username: string
}

export function useAppSession() {
    return useSession<SessionData>({
        // Session configuration
        name: 'authjs.session-token',
        password: process.env.AUTH_SECRET!, // At least 32 characters
        cookie: {
            secure: process.env.NODE_ENV === 'production',
            sameSite: 'lax',
            httpOnly: true,
        },
    })
}

export const getCurrentUserFn = createServerFn({ method: 'GET' }).handler(
    async () => {
        const session = await useAppSession()
        const userId = session.data.id

        if (!userId) {
            return null
        }

        const response = await fetch('/api/auth/session', { credentials: 'include' })
        return await response.json() as Session
    },
)