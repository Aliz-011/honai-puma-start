import { Hono } from 'hono'
import { logger } from 'hono/logger'
import { cors } from 'hono/cors'
import { Client, type SearchOptions } from 'ldapts'
import * as z from 'zod'
import { HTTPException } from "hono/http-exception";
import { authHandler, initAuthConfig, verifyAuth } from '@hono/auth-js'
import Credentials from '@auth/core/providers/credentials'

import campaign from "@/modules/campaign"
import household from "@/modules/household"
import revenueAll from "@/modules/revenue-all"
import newSales from "@/modules/new-sales"
import cvm from "@/modules/cvm"
import pv from "@/modules/pv"
import so from '@/modules/so'

const app = new Hono()

app.use(logger())
app.use('/*', cors({
    origin: [process.env.APP_URL || '', 'http://localhost:3000'],
    allowMethods: ["GET", "POST", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
    credentials: true,
}))

const loginSchema = z.object({
    username: z.string().trim().min(1, 'Please enter your username'),
    password: z.string().min(1, "Please enter your password"),
})

app.use('*',
    initAuthConfig((c) => ({
        secret: process.env.AUTH_SECRET,
        providers: [
            Credentials({
                name: 'LDAP',
                credentials: {
                    username: { label: "Username", type: "text" },
                    password: { label: "Password", type: "password" },
                },
                async authorize(credentials) {
                    const adServer = process.env.LDAP_URL as string;
                    const domain = process.env.LDAP_DOMAIN as string;

                    const rootDn = process.env.LDAP_BASE_DN as string;
                    const searchOus = (process.env.LDAP_SEARCH_OUS as string).split(',').filter(Boolean)

                    if (!adServer || !domain || !rootDn || searchOus.length === 0) {
                        console.error('LDAP config is incomplete');
                        return null
                    }

                    const client = new Client({ url: adServer })

                    try {
                        const validate = loginSchema.safeParse(credentials)
                        if (!validate.success) return null

                        const { password, username } = validate.data
                        const bindDn = `${username}${domain}`

                        await client.bind(bindDn, password)

                        let firstName = "";
                        let lastName = "";
                        let email = '';

                        for (const ou of searchOus) {
                            const baseDn = `ou=${ou},${rootDn}`
                            const searchOptions = {
                                filter: `(sAMAccountName=${username})`,
                                scope: 'sub',
                                attributes: ['givenName', 'sn', 'mail', 'name']
                            } as SearchOptions

                            const { searchEntries } = await client.search(baseDn, searchOptions)

                            for (const entry of searchEntries) {
                                firstName = Array.isArray(entry.givenName)
                                    ? (entry.givenName[0] as string | Buffer)?.toString() ?? ''
                                    : (entry.givenName as string | Buffer)?.toString() ?? '';

                                lastName = Array.isArray(entry.sn)
                                    ? (entry.sn[0] as string | Buffer)?.toString() ?? ''
                                    : (entry.sn as string | Buffer)?.toString() ?? '';

                                if (Array.isArray(entry.mail) && entry.mail[0]) {
                                    email = (entry.mail[0] as string | Buffer).toString();
                                } else if (!Array.isArray(entry.mail) && entry.mail) {
                                    email = (entry.mail as string | Buffer).toString();
                                }

                                if (firstName && lastName) break;
                            }

                            if (firstName && lastName) break;
                        }

                        const fullName = [firstName, lastName].filter(Boolean).join(' ') || username;

                        return {
                            id: `${username}-${Date.now()}`,
                            name: fullName,
                            email,
                            username
                        }
                    } catch (error) {
                        console.error('LDAP auth error:', error);
                        return null
                    } finally {
                        await client.unbind()
                    }
                },
            })
        ],
        session: {
            strategy: 'jwt',
            maxAge: 1 * 24 * 60 * 60 // 1 days
        },
        callbacks: {
            async jwt({ token, user }) {
                if (user) {
                    token.sub = user.id;
                    token.name = user.name;
                    token.email = user.email;
                    token.username = (user as any).username;
                }
                return token;
            },
            async session({ session, token }) {
                session.user.id = token.sub as string;
                session.user.name = token.name as string;
                session.user.email = token.email as string;
                // @ts-ignore
                session.user.username = token.username as string
                return session;
            },
        },
        pages: {
            signIn: "/login",
        }
    }))
)

app.use('/api/auth/*', authHandler())
app.use('/api/*', verifyAuth())

app.onError((err, c) => {
    if (err instanceof HTTPException) {
        return err.getResponse()
    }

    return c.json({ error: err.message }, 500)
})

const routes = app
    .basePath('/api')
    .route('/campaign', campaign)
    .route('/household', household)
    .route('/', revenueAll)
    .route('/', cvm)
    .route('/', newSales)
    .route('/', pv)
    .route('/', so)

export default app
export type AppType = typeof routes;