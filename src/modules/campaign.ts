import { Hono } from "hono";
import * as z from 'zod'
import { zValidator } from '@hono/zod-validator'
import { format, getDaysInMonth, subDays } from 'date-fns'
import { and, asc, eq, gt, isNull, like, not, sql } from "drizzle-orm";
import { stream } from 'hono/streaming'

import { dynamicMultidim } from "@/db/schema/multidim";
import { dynamicChannelWaBroadband, wifi_lapser_prevention_puma } from "@/db/schema/zz_denny";
import { fei_wl_compete_puma_analysis, fei_actual_wl_trade_in } from '@/db/schema/fei'
import { db } from "@/db";
import { territoryArea4 } from "@/db/schema/puma_2025";

// Escape CSV value - inline for performance
const escapeCSV = (value: unknown): string => {
    if (value === null || value === undefined) return ''
    const str = String(value)
    if (str.includes(',') || str.includes('\n') || str.includes('"')) {
        return `"${str.replace(/"/g, '""')}"`
    }
    return str
}

const campaignQuerySchema = z.object({
    date: z.string().optional(),
    branch: z.string().optional(),
    cluster: z.string().optional(),
    kabupaten: z.string().optional(),
    kecamatan: z.string().optional(),
    method: z.string().optional(),
    product_offer: z.string().optional(),
    rows: z.string().optional()
})

const buildWLCampaignQuery = (params: z.infer<typeof campaignQuerySchema>) => {
    const { date, branch, method, product_offer, cluster, kabupaten, kecamatan, rows } = params

    const currentTime = date ? new Date(date) : subDays(new Date(), 2)
    const period = format(currentTime, 'yyyyMM')
    const daysInMonth = getDaysInMonth(currentTime)

    const multidim = dynamicMultidim(period)
    const channelWaBroadband = dynamicChannelWaBroadband(period)

    const conditions = [
        not(eq(multidim.brand, 'KartuHalo')),
        eq(multidim.region_sales, 'PUMA'),
        isNull(channelWaBroadband.Penawaran),
        eq(multidim.rev_data_mtd, '0'),
        gt(multidim.rev_data_m2, '0'),
        eq(multidim.rev_m1, '0')
    ]

    if (branch) conditions.push(eq(multidim.branch, branch))
    if (cluster) conditions.push(eq(multidim.cluster_sales, cluster))
    if (kabupaten) conditions.push(eq(multidim.kabupaten, kabupaten))
    if (kecamatan) conditions.push(eq(multidim.kecamatan, kecamatan))
    if (method === 'wa') conditions.push(like(multidim.bcp_interest_chat, '%WhatsApp%'))

    // Product offer filtering - push conditions directly into WHERE
    if (product_offer === 'wifi_lapser') {
        conditions.push(sql`${wifi_lapser_prevention_puma.segment} IS NOT NULL`)
    } else if (product_offer === 'trade_in') {
        conditions.push(sql`${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NOT NULL`)
    } else if (product_offer === 'ss_compete') {
        conditions.push(sql`${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NULL AND ${fei_wl_compete_puma_analysis.kecamatan} IS NOT NULL`)
    } else if (product_offer === 'pelanggan_baru') {
        conditions.push(sql`${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NULL AND ${fei_wl_compete_puma_analysis.kecamatan} IS NULL AND ${multidim.los} < 90`)
    } else if (product_offer === 'lifestage_3') {
        conditions.push(sql`${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NULL AND ${fei_wl_compete_puma_analysis.kecamatan} IS NULL AND ${multidim.los} > 90 AND ${multidim.interim_lifestage} = 3`)
    } else if (product_offer === 'reguler_package') {
        conditions.push(sql`${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NULL AND ${fei_wl_compete_puma_analysis.kecamatan} IS NULL AND ${multidim.los} > 90 AND ${multidim.interim_lifestage} != 3`)
    }

    let query = db
        .select({
            msisdn: multidim.msisdn,
            site_id: multidim.site_id,
            kecamatan: multidim.kecamatan,
            kabupaten: multidim.kabupaten,
            branch: territoryArea4.branch,

            product_offer: sql<string>`CASE 
                WHEN ${wifi_lapser_prevention_puma.segment} IS NOT NULL THEN 'Wifi lapser'
                WHEN ${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NOT NULL THEN 'trade-in'
                WHEN ${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NULL AND ${fei_wl_compete_puma_analysis.kecamatan} IS NOT NULL THEN 'SS Compete'
                WHEN ${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NULL AND ${fei_wl_compete_puma_analysis.kecamatan} IS NULL AND ${multidim.los} < 90 THEN 'pelanggan baru'
                WHEN ${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NULL AND ${fei_wl_compete_puma_analysis.kecamatan} IS NULL AND ${multidim.los} > 90 AND ${multidim.interim_lifestage} = 3 THEN 'lifestage 3'
                WHEN ${wifi_lapser_prevention_puma.segment} IS NULL AND ${fei_actual_wl_trade_in.kota} IS NULL AND ${fei_wl_compete_puma_analysis.kecamatan} IS NULL AND ${multidim.los} > 90 AND ${multidim.interim_lifestage} != 3 THEN 'reguler package'
            END`.as('product_offer'),

            denom_offer: sql<string>`CASE
                WHEN ${multidim.rev_data_stretch_mtd} BETWEEN 0 AND 50000 THEN 'low denom'
                WHEN ${multidim.rev_data_stretch_mtd} > 50000 THEN 'high denom'
            END `.as('denom_offer'),

            status: multidim.status,

            campaign_method: sql<string>`CASE
                WHEN ${multidim.bcp_interest_chat} LIKE '%WhatsApp%' THEN 'WA Blast' ELSE 'SMS Blast'
            END`.as('campaign_method'),

            wl_category: sql<string>`CASE
                WHEN ${multidim.rev_data_m1} = 0 THEN 'Non RGB M1'
                WHEN ${multidim.rev_data_m1} > 0 AND ${multidim.rev_data_mtd_m1} = 0 THEN 'Non RGB MTD1'
                WHEN ${multidim.rev_data_m1} > 0 AND ${multidim.rev_data_mtd_m1} > 0 THEN 'uplift'
            END`.as('wl_category'),

            priority: sql<string>`CASE 
                WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_m1} / ${daysInMonth}) BETWEEN 0 AND 14 THEN 1
                WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_m1} / ${daysInMonth}) > 14 THEN 2
                WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_m1} / ${daysInMonth}) IS NULL AND ${multidim.days_expiry_data} BETWEEN 0 AND 14 THEN 3
                WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_m1} / ${daysInMonth}) IS NULL AND ${multidim.days_expiry_data} > 14 THEN 4
                WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_m1} / ${daysInMonth}) IS NULL AND ${multidim.days_expiry_data} IS NULL AND ${multidim.rev_data_mtd_m1} > 0 THEN 6
                WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_m1} / ${daysInMonth}) IS NULL AND ${multidim.days_expiry_data} IS NULL AND ${multidim.rev_data_mtd_m1} = 0 THEN 7
                WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_m1} / ${daysInMonth}) IS NULL AND ${multidim.days_expiry_data} IS NULL AND ${multidim.rev_data_mtd_m1} = 0 AND ${multidim.rev_data_avg_l3m} > 0 THEN 8
            ELSE 8 END`.as('priority')
        })
        .from(multidim)
        .leftJoin(territoryArea4, eq(multidim.kabupaten, territoryArea4.kabupaten))
        .leftJoin(wifi_lapser_prevention_puma, and(eq(multidim.msisdn, wifi_lapser_prevention_puma.msisdn), eq(wifi_lapser_prevention_puma.periode, sql`(SELECT MAX(${wifi_lapser_prevention_puma.periode}) FROM ${wifi_lapser_prevention_puma})`)))
        .leftJoin(fei_actual_wl_trade_in, eq(multidim.msisdn, fei_actual_wl_trade_in.msisdn_trade_in))
        .leftJoin(fei_wl_compete_puma_analysis, and(eq(multidim.msisdn, fei_wl_compete_puma_analysis.msisdn), eq(fei_wl_compete_puma_analysis.periode, sql`(SELECT MAX(${fei_wl_compete_puma_analysis.periode}) FROM ${fei_wl_compete_puma_analysis})`)))
        .leftJoin(channelWaBroadband, eq(multidim.msisdn, channelWaBroadband.MSISDN_Pelanggan))
        .where(and(...conditions))
        .orderBy(asc(sql`priority`))

    if (rows && parseInt(rows) > 0) {
        query = query.limit(parseInt(rows)) as any
    }

    return query
}

const app = new Hono()
    .get('/wl-campaign', zValidator('query', campaignQuerySchema),
        async c => {
            const params = c.req.valid('query')

            // Default limit for API response (smaller than download)
            if (!params.rows) params.rows = '1000'

            const query = buildWLCampaignQuery(params)
            const listWl = await query

            console.log(`[WL Campaign API] Query returned ${listWl.length} rows`)

            return c.json(
                { data: listWl, count: listWl.length },
                200,
                {
                    'Cache-Control': 'public, max-age=60', // 1 minute cache
                }
            )
        })
    .get('/wl-campaign/download', zValidator('query', campaignQuerySchema),
        async c => {
            const params = c.req.valid('query')

            // Default limit to prevent server overload
            if (!params.rows) params.rows = '30000'

            const timestamp = format(new Date(), 'yyyyMMdd')
            const filterParts = [
                params.branch && `branch-${params.branch}`,
                params.cluster && `cluster-${params.cluster}`,
                params.method && `method-${params.method}`,
                params.product_offer && `offer-${params.product_offer}`
            ].filter(Boolean).join('_')

            const filename = `wl_campaign_${timestamp}${filterParts ? '_' + filterParts : ''}.csv`

            // Set headers BEFORE streaming
            c.header('Content-Type', 'text/csv; charset=utf-8')
            c.header('Content-Disposition', `attachment; filename="${filename}"`)

            // Stream CSV directly - much lighter on memory
            return stream(c, async (stream) => {
                const query = buildWLCampaignQuery(params)
                const listWl = await query

                console.log(`[WL Campaign] Query returned ${listWl.length} rows`)

                if (listWl.length === 0) {
                    await stream.write('No data found')
                    return
                }

                // Write CSV header
                const headers = Object.keys(listWl[0])
                await stream.write(headers.join(',') + '\n')

                // Process in chunks to reduce memory pressure
                const CHUNK_SIZE = 500
                for (let i = 0; i < listWl.length; i += CHUNK_SIZE) {
                    const chunk = listWl.slice(i, Math.min(i + CHUNK_SIZE, listWl.length))
                    const csvLines = chunk.map(row =>
                        headers.map(h => escapeCSV(row[h as keyof typeof row])).join(',')
                    ).join('\n') + '\n'
                    await stream.write(csvLines)
                }
            })
        })

export default app