import { Hono } from 'hono'
import { logger } from 'hono/logger'
import { cors } from 'hono/cors'
import { Client, type SearchOptions } from 'ldapts'
import { max, eq } from 'drizzle-orm'
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
import rgb from "@/modules/paying-subs"
import { summaryBbCity, summaryRevAllByLosKabupaten, summaryRevAllKabupaten, summaryRgbHqKabupaten, summarySoAllKabupaten } from './schema/v_honai_puma'
import { db } from '.'
import { territoryArea4 } from './schema/puma_2025'

const app = new Hono()

app.use(logger())
app.use('/*', cors({
    origin: [process.env.APP_URL || '', 'http://localhost:3000', 'http://10.113.4.55'],
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
        secret: process.env.AUTH_SECRET!,
        trustHost: true,
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
                    const searchOus = "Medan,Batam,Palembang,Atrium,Bandung,Surabaya,Semarang,Denpasar,UjungPandang,Balikpapan,HeadOffice".split(',').filter(Boolean)

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

                        console.log('AUTH_SECRET exists:', !!process.env.AUTH_SECRET)
                        console.log('LDAP_URL:', process.env.LDAP_URL)

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
            maxAge: 3 * 60 * 60 // 3 Hours
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
    .route('/', rgb)
    .get('/max-date', async (c) => {
        const gross = db
            .select({
                regional: summaryRevAllKabupaten.regional,
                tgl_gross: max(summaryRevAllKabupaten.tgl).as('tgl_gross')
            })
            .from(summaryRevAllKabupaten)
            .groupBy(summaryRevAllKabupaten.regional)
            .as('gross')

        const ns = db
            .select({
                regional: summaryRevAllByLosKabupaten.regional,
                tgl_ns: max(summaryRevAllByLosKabupaten.tgl).as('tgl_ns')
            })
            .from(summaryRevAllByLosKabupaten)
            .groupBy(summaryRevAllByLosKabupaten.regional)
            .as('ns')

        const cvm = db
            .select({
                regional: summaryBbCity.regional,
                tgl_cvm: max(summaryBbCity.tgl).as('tgl_cvm')
            })
            .from(summaryBbCity)
            .groupBy(summaryBbCity.regional)
            .as('cvm')

        const rgb = db
            .select({
                regional: summaryRgbHqKabupaten.regional,
                tgl_rgb: max(summaryRgbHqKabupaten.event_date).as('tgl_rgb')
            })
            .from(summaryRgbHqKabupaten)
            .groupBy(summaryRgbHqKabupaten.regional)
            .as('rgb')

        const so = db
            .select({
                regional: summarySoAllKabupaten.regional,
                tgl_so: max(summarySoAllKabupaten.tgl).as('tgl_so')
            })
            .from(summarySoAllKabupaten)
            .groupBy(summarySoAllKabupaten.regional)
            .as('so')

        const regionalSubquery = db
            .selectDistinct({
                regional: territoryArea4.regional
            })
            .from(territoryArea4)
            .where(eq(territoryArea4.regional, 'PUMA'))
            .as('regional')

        const [finalData] = await db
            .select({
                regional: regionalSubquery.regional,
                tgl_gross: gross.tgl_gross,
                tgl_ns: ns.tgl_ns,
                tgl_cvm: cvm.tgl_cvm,
                tgl_rgb: rgb.tgl_rgb,
                tgl_so: so.tgl_so
            })
            .from(regionalSubquery)
            .leftJoin(rgb, eq(regionalSubquery.regional, rgb.regional))
            .leftJoin(gross, eq(regionalSubquery.regional, gross.regional))
            .leftJoin(ns, eq(regionalSubquery.regional, ns.regional))
            .leftJoin(cvm, eq(regionalSubquery.regional, cvm.regional))
            .leftJoin(so, eq(regionalSubquery.regional, so.regional))

        return c.json(finalData)
    })

export default app
export type AppType = typeof routes;