import { Hono } from "hono";
import * as z from 'zod'
import { zValidator } from '@hono/zod-validator'
import { format, subDays } from 'date-fns'
import { dynamicMultidim } from "@/db/schema/multidim";
import { dynamicChannelWaBroadband, dynamicWLWABLASTBroadband } from "@/db/schema/zz_denny";
import { db } from "@/db";
import { and, asc, count, countDistinct, eq, gt, isNull, like, not, sql } from "drizzle-orm";
import AdmZip from 'adm-zip'

const convertToCSV = (data: any[]) => {
    if (!data.length) return ''

    const headers = Object.keys(data[0])
    const csvHeaders = headers.join(',')

    const csvRows = data.map(row =>
        headers.map(header => {
            const value = row[header]
            // Escape quotes and wrap in quotes if contains comma or newline
            if (value === null || value === undefined) return ''
            const stringValue = String(value)
            if (stringValue.includes(',') || stringValue.includes('\n') || stringValue.includes('"')) {
                return `"${stringValue.replace(/"/g, '""')}"`
            }
            return stringValue
        }).join(',')
    ).join('\n')

    return `${csvHeaders}\n${csvRows}`
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

    const multidim = dynamicMultidim(period)
    const wlWaBlast = dynamicWLWABLASTBroadband(period)
    const channelWaBroadband = dynamicChannelWaBroadband(period)

    const conditions = [
        not(eq(multidim.brand, 'KartuHalo')),
        eq(multidim.region_sales, 'PUMA'),
        isNull(channelWaBroadband.Penawaran),
        eq(multidim.rev_data_mtd, '0'),
    ]

    if (branch) conditions.push(eq(multidim.branch, branch))
    if (cluster) conditions.push(eq(wlWaBlast.cluster, cluster))
    if (kabupaten) conditions.push(eq(multidim.kabupaten, kabupaten))
    if (kecamatan) conditions.push(eq(multidim.kecamatan, kecamatan))
    if (method === 'wa') conditions.push(like(multidim.bcp_interest_chat, '%WhatsApp%'))
    if (product_offer === 'slm_lifestage_3') {
        conditions.push(gt(multidim.los, 90), eq(multidim.interim_lifestage, 3))
    }

    let query = db
        .select({
            msisdn: multidim.msisdn,
            cluster: wlWaBlast.cluster,
            city: wlWaBlast.city,
            los_cat: sql<string>`CASE WHEN ${multidim.los} < 90 THEN '< 90d' ELSE '> 90d' END`.as('los_cat'),
            product_offer: sql<string>`${product_offer}`.as('product_offer'),
            priority: sql<string>`CONCAT(
                CASE WHEN ${wlWaBlast.cluster} IS NOT NULL THEN 'P1' ELSE 'P2' END,
                CASE WHEN ${multidim.device_type} IN ('Mobile Phone/Feature phone','Smartphone','Tablet') THEN 'P1' ELSE 'P2' END,
                CASE WHEN ${multidim.status} IN ('A','G','E') THEN 'P1' ELSE 'P2' END,
                CASE WHEN ${multidim.rev_data_mtd_m1} > 0 THEN 'P1' ELSE 'P2' END,
                CASE WHEN ${multidim.vol_data_mtd} > 0 THEN 'P1' ELSE 'P2' END,
                CASE 
                    WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_mtd} / 26) <3 THEN 'P1'
                    WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_mtd} / 26) BETWEEN 3 AND 7 THEN 'P2'
                    WHEN ${multidim.vol_data_pack_remain} / (${multidim.vol_data_mtd} / 26) >7 THEN 'P3'
                    ELSE 'P4' 
                END
            )`.as('priority')
        })
        .from(multidim)
        .leftJoin(wlWaBlast, eq(multidim.msisdn, wlWaBlast.msisdn))
        .leftJoin(channelWaBroadband, eq(multidim.msisdn, channelWaBroadband.MSISDN_Pelanggan))
        .where(and(...conditions))
        .orderBy(asc(sql`priority`))

    if (rows) {
        query = query.limit(parseInt(rows)) as any
    }

    return query
}

const app = new Hono()
    .get('/campaign-achievement', zValidator('query', z.object({ branch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const params = c.req.valid('query')

            const currentTime = subDays(new Date(), 2)
            const period = format(currentTime, 'yyyyMM')
            const { cluster, kabupaten, branch } = params

            const multidim = dynamicMultidim(period)
            const wlWaBlast = dynamicWLWABLASTBroadband(period)
            const channelWaBroadband = dynamicChannelWaBroadband(period)

            const conditions = [
                eq(wlWaBlast.region, 'PUMA')
            ]

            if (branch) conditions.push(eq(wlWaBlast.branch, branch))
            if (cluster) conditions.push(eq(wlWaBlast.cluster, cluster))
            if (kabupaten) conditions.push(eq(wlWaBlast.city, kabupaten))

            const subqueryMultidim = db
                .select({
                    msisdn: multidim.msisdn,
                    rev_data_mtd: multidim.rev_data_mtd,
                    rev_data_m1: multidim.rev_data_m1,
                    rev_data_pack_mtd: multidim.rev_data_pack_mtd,
                    rev_data_pack_m1: multidim.rev_data_pack_m1,
                    rev_data_pack_mtd_m1: multidim.rev_data_pack_mtd_m1,
                    gap_rev_data_mtd: sql<string>`${multidim.rev_data_mtd} - ${multidim.rev_data_m1}`.as('gap_rev_data_mtd')
                })
                .from(multidim)
                .as('subqueryMultidim')

            const subqueryWlWaBlast = db
                .select({
                    msisdn: wlWaBlast.msisdn,
                    branch: wlWaBlast.branch,
                    cluster: wlWaBlast.cluster,
                    segment_los: wlWaBlast.segment_los
                })
                .from(wlWaBlast)
                .where(and(...conditions))
                .groupBy(sql`1,2,3,4`)
                .as('subqueryWlWaBlast')

            const achievements = await db
                .select({
                    cluster: subqueryWlWaBlast.cluster,
                    champion: sql<string>`CASE WHEN ${channelWaBroadband.Penawaran} IS NOT NULL THEN 'Y' ELSE 'N' END`.as('champion'),
                    segment_los: subqueryWlWaBlast.segment_los,
                    trx: count(subqueryWlWaBlast.msisdn),
                    subs: countDistinct(subqueryWlWaBlast.msisdn)
                })
                .from(subqueryWlWaBlast)
                .leftJoin(channelWaBroadband, eq(subqueryWlWaBlast.msisdn, channelWaBroadband.MSISDN_Pelanggan))
                .leftJoin(subqueryMultidim, eq(subqueryWlWaBlast.msisdn, subqueryMultidim.msisdn))
                .groupBy(sql`1,2,3`)

            return c.json(
                { data: achievements },
                200,
                {
                    'Cache-Control': 'public, max-age=300', // 5 minutes
                }
            )
        })
    .get('/wl-campaign/download', zValidator('query', campaignQuerySchema),
        async c => {
            const params = c.req.valid('query')

            const listWl = await buildWLCampaignQuery(params)

            const csv = convertToCSV(listWl)

            const timestamp = format(new Date(), 'yyyyMMdd')
            const filterParts = [
                params.branch && `branch-${params.branch}`,
                params.cluster && `cluster-${params.cluster}`,
                params.method && `method-${params.method}`,
                params.product_offer && `offer-${params.product_offer}`
            ].filter(Boolean).join('_')

            const filename = `wl_campaign_${timestamp}${filterParts ? '_' + filterParts : ''}.csv`
            const zipFilename = `wl_campaign_${timestamp}${filterParts ? '_' + filterParts : ''}.zip`

            const zip = new AdmZip()
            zip.addFile(filename, Buffer.from(csv, 'utf-8'))
            const zipBuffer = zip.toBuffer()

            return c.body(new Uint8Array(zipBuffer), 200, {
                'Content-Type': 'application/zip',
                'Content-Disposition': `attachment; filename="${zipFilename}"`,
                'Cache-Control': 'no-cache',
            })
        })

export default app