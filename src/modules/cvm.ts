import { format, getDaysInMonth, subDays } from "date-fns";
import { and, eq, sql, sum, max } from "drizzle-orm";
import { unionAll } from 'drizzle-orm/mysql-core'
import { Hono } from "hono";
import { z } from "zod"
import { zValidator } from "@hono/zod-validator";


import { summaryBbRegional, summaryBbBranch, summaryBbSubbranch, summaryBbCluster, summaryBbCity, feiTargetPuma } from "@/db/schema/v_honai_puma";
import { territoryArea4 } from '@/db/schema/puma_2025'
import { db } from "@/db";

const app = new Hono()
    .get('/revenue-cvm-v2', zValidator('query', z.object({ date: z.coerce.date().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const currDateYYMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalSubquery = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select({
                    regional: summaryBbRegional.regional,
                    Rev_BTL: max(summaryBbRegional.Rev_BTL).as('revBTL'),
                    MoM_BTL: max(summaryBbRegional.MoM_BTL).as('momBTL'),
                    Abs_BTL: max(summaryBbRegional.Abs_BTL).as('absBTL'),
                    YoY_BTL: max(summaryBbRegional.YoY_BTL).as('yoyBTL'),
                    YTD_BTL: max(summaryBbRegional.YTD_BTL).as('ytdBTL'),
                    QoQ_BTL: max(summaryBbRegional.QoQ_BTL).as('qoqBTL'),
                })
                .from(summaryBbRegional)
                .where(and(
                    eq(summaryBbRegional.tgl, currDate),
                    eq(summaryBbRegional.regional, 'PUMA'),
                    eq(summaryBbRegional.type, 'all')
                ))
                .groupBy(summaryBbRegional.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_rev_cvm: sum(feiTargetPuma.rev_cvm).as('target_rev_cvm')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalSubquery.regional,
                    targetAll: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_rev_cvm} / 1000000000), 2)`.as('target_rev_cvm'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevRegional.Rev_BTL}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.Rev_BTL}) / SUM(${regionalTargetRevenue.target_rev_cvm} / 1000000000)) * 100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.Rev_BTL}) / (${today} / ${daysInMonth} * SUM(${regionalTargetRevenue.target_rev_cvm} / 1000000000))) * 100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevRegional.Rev_BTL}) - SUM(${regionalTargetRevenue.target_rev_cvm} / 1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevRegional.MoM_BTL}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevRegional.Abs_BTL}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevRegional.YoY_BTL}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevRegional.YTD_BTL}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevRegional.QoQ_BTL}, '%')`.as('qoq_cvm'),
                })
                .from(regionalSubquery)
                .leftJoin(summaryRevRegional, eq(regionalSubquery.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalSubquery.regional, regionalTargetRevenue.regional))
                .groupBy(regionalSubquery.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    qoqAll: sql<string>`''`
                })
                .from(feiTargetPuma)

            const branchSubquery = db
                .select({ branch: territoryArea4.branch })
                .from(territoryArea4)
                .where(and(
                    eq(territoryArea4.regional, 'PUMA'),
                    branch ? eq(territoryArea4.branch, branch) : undefined
                ))
                .groupBy(territoryArea4.branch)
                .as('a')

            const summaryRevBranch = db
                .select({
                    branch: summaryBbBranch.branch,
                    Rev_BTL: max(summaryBbBranch.Rev_BTL).as('revBTL'),
                    MoM_BTL: max(summaryBbBranch.MoM_BTL).as('momBTL'),
                    Abs_BTL: max(summaryBbBranch.Abs_BTL).as('absBTL'),
                    YoY_BTL: max(summaryBbBranch.YoY_BTL).as('yoyBTL'),
                    YTD_BTL: max(summaryBbBranch.YTD_BTL).as('ytdBTL'),
                    QoQ_BTL: max(summaryBbBranch.QoQ_BTL).as('qoqBTL'),
                })
                .from(summaryBbBranch)
                .where(and(
                    eq(summaryBbBranch.tgl, currDate),
                    eq(summaryBbBranch.regional, 'PUMA'),
                    eq(summaryBbBranch.type, 'all')
                ))
                .groupBy(summaryBbBranch.branch)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_rev_cvm: sum(feiTargetPuma.rev_cvm).as('target_rev_cvm')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchSubquery.branch,
                    targetAll: sql<number>`ROUND(SUM(${branchTargetRevenue.target_rev_cvm}/1000000000), 2)`.as('target_rev_cvm'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevBranch.Rev_BTL}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.Rev_BTL})/SUM(${branchTargetRevenue.target_rev_cvm}/1000000000))*100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.Rev_BTL})/(${today}/${daysInMonth}*SUM(${branchTargetRevenue.target_rev_cvm}/1000000000)))*100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevBranch.Rev_BTL}) - SUM(${branchTargetRevenue.target_rev_cvm}/1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevBranch.MoM_BTL}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevBranch.Abs_BTL}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevBranch.YoY_BTL}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevBranch.YTD_BTL}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevBranch.QoQ_BTL}, '%')`.as('qoq_cvm'),
                })
                .from(branchSubquery)
                .leftJoin(summaryRevBranch, eq(branchSubquery.branch, summaryRevBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchSubquery.branch, branchTargetRevenue.branch))
                .groupBy(branchSubquery.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    qoqAll: sql<string>`''`
                })
                .from(feiTargetPuma)

            const subbranchSubquery = db
                .select({ subbranch: territoryArea4.subbranch })
                .from(territoryArea4)
                .where(branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch)
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryBbSubbranch.subbranch,
                    Rev_BTL: max(summaryBbSubbranch.Rev_BTL).as('revBTL'),
                    MoM_BTL: max(summaryBbSubbranch.MoM_BTL).as('momBTL'),
                    Abs_BTL: max(summaryBbSubbranch.Abs_BTL).as('absBTL'),
                    YoY_BTL: max(summaryBbSubbranch.YoY_BTL).as('yoyBTL'),
                    YTD_BTL: max(summaryBbSubbranch.YTD_BTL).as('ytdBTL'),
                    QoQ_BTL: max(summaryBbSubbranch.QoQ_BTL).as('qoqBTL'),
                })
                .from(summaryBbSubbranch)
                .where(and(
                    eq(summaryBbSubbranch.tgl, currDate),
                    eq(summaryBbSubbranch.regional, 'PUMA'),
                    eq(summaryBbSubbranch.type, 'all')
                ))
                .groupBy(summaryBbSubbranch.subbranch)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_rev_cvm: sum(feiTargetPuma.rev_cvm).as('target_rev_cvm')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchSubquery.subbranch,
                    targetAll: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_rev_cvm}/1000000000), 2)`.as('target_rev_cvm'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.Rev_BTL}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.Rev_BTL})/SUM(${subbranchTargetRevenue.target_rev_cvm}/1000000000))*100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.Rev_BTL})/(${today}/${daysInMonth}*SUM(${subbranchTargetRevenue.target_rev_cvm}/1000000000)))*100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevSubbranch.Rev_BTL}) - SUM(${subbranchTargetRevenue.target_rev_cvm}/1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevSubbranch.MoM_BTL}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevSubbranch.Abs_BTL}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevSubbranch.YoY_BTL}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevSubbranch.YTD_BTL}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevSubbranch.QoQ_BTL}, '%')`.as('qoq_cvm'),
                })
                .from(subbranchSubquery)
                .leftJoin(summaryRevSubbranch, eq(subbranchSubquery.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchSubquery.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchSubquery.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    qoqAll: sql<string>`''`
                })
                .from(feiTargetPuma)

            const clusterSubquery = db
                .select({ cluster: territoryArea4.cluster })
                .from(territoryArea4)
                .where(branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.cluster)
                .as('a')

            const summaryRevCluster = db
                .select({
                    cluster: summaryBbCluster.cluster,
                    Rev_BTL: max(summaryBbCluster.Rev_BTL).as('revBTL'),
                    MoM_BTL: max(summaryBbCluster.MoM_BTL).as('momBTL'),
                    Abs_BTL: max(summaryBbCluster.Abs_BTL).as('absBTL'),
                    YoY_BTL: max(summaryBbCluster.YoY_BTL).as('yoyBTL'),
                    YTD_BTL: max(summaryBbCluster.YTD_BTL).as('ytdBTL'),
                    QoQ_BTL: max(summaryBbCluster.QoQ_BTL).as('qoqBTL'),
                })
                .from(summaryBbCluster)
                .where(and(
                    eq(summaryBbCluster.tgl, currDate),
                    eq(summaryBbCluster.regional, 'PUMA'),
                    eq(summaryBbCluster.type, 'all')
                ))
                .groupBy(summaryBbCluster.cluster)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_rev_cvm: sum(feiTargetPuma.rev_cvm).as('target_rev_cvm')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterSubquery.cluster,
                    targetAll: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_rev_cvm}/1000000000), 2)`.as('target_rev_cvm'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCluster.Rev_BTL}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.Rev_BTL})/SUM(${clusterTargetRevenue.target_rev_cvm}/1000000000))*100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.Rev_BTL})/(${today}/${daysInMonth}*SUM(${clusterTargetRevenue.target_rev_cvm}/1000000000)))*100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevCluster.Rev_BTL}) - SUM(${clusterTargetRevenue.target_rev_cvm}/1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevCluster.MoM_BTL}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevCluster.Abs_BTL}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevCluster.YoY_BTL}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevCluster.YTD_BTL}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevCluster.QoQ_BTL}, '%')`.as('qoq_cvm'),
                })
                .from(clusterSubquery)
                .leftJoin(summaryRevCluster, eq(clusterSubquery.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterSubquery.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterSubquery.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    qoqAll: sql<string>`''`
                })
                .from(feiTargetPuma)

            const kabupatenSubquery = db
                .select({ kabupaten: territoryArea4.kabupaten })
                .from(territoryArea4)
                .where(branch && subbranch && cluster && kabupaten ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                    eq(territoryArea4.kabupaten, kabupaten),
                ) : branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.kabupaten)
                .as('a')

            const summaryRevCity = db
                .select({
                    city: summaryBbCity.city,
                    Rev_BTL: max(summaryBbCity.Rev_BTL).as('revBTL'),
                    MoM_BTL: max(summaryBbCity.MoM_BTL).as('momBTL'),
                    Abs_BTL: max(summaryBbCity.Abs_BTL).as('absBTL'),
                    YoY_BTL: max(summaryBbCity.YoY_BTL).as('yoyBTL'),
                    YTD_BTL: max(summaryBbCity.YTD_BTL).as('ytdBTL'),
                    QoQ_BTL: max(summaryBbCity.QoQ_BTL).as('qoqBTL'),
                })
                .from(summaryBbCity)
                .where(and(
                    eq(summaryBbCity.tgl, currDate),
                    eq(summaryBbCity.regional, 'PUMA'),
                    eq(summaryBbCity.type, 'all')
                ))
                .groupBy(summaryBbCity.city)
                .as('b')

            const cityTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_rev_cvm: sum(feiTargetPuma.rev_cvm).as('target_rev_cvm')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueCity = db
                .select({
                    name: kabupatenSubquery.kabupaten,
                    targetAll: sql<number>`ROUND(SUM(${cityTargetRevenue.target_rev_cvm}/1000000000), 2)`.as('target_rev_cvm'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCity.Rev_BTL}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCity.Rev_BTL})/SUM(${cityTargetRevenue.target_rev_cvm}/1000000000))*100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCity.Rev_BTL})/(${today}/${daysInMonth}*SUM(${cityTargetRevenue.target_rev_cvm}/1000000000)))*100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevCity.Rev_BTL}) - SUM(${cityTargetRevenue.target_rev_cvm}/1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevCity.MoM_BTL}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevCity.Abs_BTL}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevCity.YoY_BTL}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevCity.YTD_BTL}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevCity.QoQ_BTL}, '%')`.as('qoq_cvm'),
                })
                .from(kabupatenSubquery)
                .leftJoin(summaryRevCity, eq(kabupatenSubquery.kabupaten, summaryRevCity.city))
                .leftJoin(cityTargetRevenue, eq(kabupatenSubquery.kabupaten, cityTargetRevenue.kabupaten))
                .groupBy(kabupatenSubquery.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueCity)
            ])

            return c.json(finalDataRevenue, 200)
        })
    .get('/revenue-cvm-outlet-v2', zValidator('query', z.object({ date: z.coerce.date().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const currDateYYMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalSubquery = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select({
                    regional: summaryBbRegional.regional,
                    Rev_MKIOS_Channel: max(summaryBbRegional.Rev_MKIOS_Channel).as('revMKIOS_Channel'),
                    MoM_MKIOS_Channel: max(summaryBbRegional.MoM_MKIOS_Channel).as('momMKIOS_Channel'),
                    Abs_MKIOS_Channel: max(summaryBbRegional.Abs_MKIOS_Channel).as('absMKIOS_Channel'),
                    YoY_MKIOS_Channel: max(summaryBbRegional.YoY_MKIOS_Channel).as('yoyMKIOS_Channel'),
                    YTD_MKIOS_Channel: max(summaryBbRegional.YTD_MKIOS_Channel).as('ytdMKIOS_Channel'),
                    QoQ_MKIOS_Channel: max(summaryBbRegional.QoQ_MKIOS_Channel).as('qoqMKIOS_Channel'),
                })
                .from(summaryBbRegional)
                .where(and(
                    eq(summaryBbRegional.tgl, currDate),
                    eq(summaryBbRegional.regional, 'PUMA'),
                    eq(summaryBbRegional.type, 'all')
                ))
                .groupBy(summaryBbRegional.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_rev_cvm_outlet: sum(feiTargetPuma.rev_cvm_outlet).as('target_rev_cvm_outlet')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalSubquery.regional,
                    targetAll: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_rev_cvm_outlet} / 1000000000), 2)`.as('target_rev_cvm_outlet'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevRegional.Rev_MKIOS_Channel}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.Rev_MKIOS_Channel}) / SUM(${regionalTargetRevenue.target_rev_cvm_outlet} / 1000000000)) * 100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.Rev_MKIOS_Channel}) / (${today} / ${daysInMonth} * SUM(${regionalTargetRevenue.target_rev_cvm_outlet} / 1000000000))) * 100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevRegional.Rev_MKIOS_Channel}) - SUM(${regionalTargetRevenue.target_rev_cvm_outlet} / 1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevRegional.MoM_MKIOS_Channel}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevRegional.Abs_MKIOS_Channel}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevRegional.YoY_MKIOS_Channel}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevRegional.YTD_MKIOS_Channel}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevRegional.QoQ_MKIOS_Channel}, '%')`.as('qoq_cvm'),
                })
                .from(regionalSubquery)
                .leftJoin(summaryRevRegional, eq(regionalSubquery.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalSubquery.regional, regionalTargetRevenue.regional))
                .groupBy(regionalSubquery.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    qoqAll: sql<string>`''`
                })
                .from(feiTargetPuma)

            const branchSubquery = db
                .select({ branch: territoryArea4.branch })
                .from(territoryArea4)
                .where(and(
                    eq(territoryArea4.regional, 'PUMA'),
                    branch ? eq(territoryArea4.branch, branch) : undefined
                ))
                .groupBy(territoryArea4.branch)
                .as('a')

            const summaryRevBranch = db
                .select({
                    branch: summaryBbBranch.branch,
                    Rev_MKIOS_Channel: max(summaryBbBranch.Rev_MKIOS_Channel).as('revMKIOS_Channel'),
                    MoM_MKIOS_Channel: max(summaryBbBranch.MoM_MKIOS_Channel).as('momMKIOS_Channel'),
                    Abs_MKIOS_Channel: max(summaryBbBranch.Abs_MKIOS_Channel).as('absMKIOS_Channel'),
                    YoY_MKIOS_Channel: max(summaryBbBranch.YoY_MKIOS_Channel).as('yoyMKIOS_Channel'),
                    YTD_MKIOS_Channel: max(summaryBbBranch.YTD_MKIOS_Channel).as('ytdMKIOS_Channel'),
                    QoQ_MKIOS_Channel: max(summaryBbBranch.QoQ_MKIOS_Channel).as('qoqMKIOS_Channel'),
                })
                .from(summaryBbBranch)
                .where(and(
                    eq(summaryBbBranch.tgl, currDate),
                    eq(summaryBbBranch.regional, 'PUMA'),
                    eq(summaryBbBranch.type, 'all')
                ))
                .groupBy(summaryBbBranch.branch)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_rev_cvm_outlet: sum(feiTargetPuma.rev_cvm_outlet).as('target_rev_cvm_outlet')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchSubquery.branch,
                    targetAll: sql<number>`ROUND(SUM(${branchTargetRevenue.target_rev_cvm_outlet}/1000000000), 2)`.as('target_rev_cvm_outlet'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevBranch.Rev_MKIOS_Channel}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.Rev_MKIOS_Channel})/SUM(${branchTargetRevenue.target_rev_cvm_outlet}/1000000000))*100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.Rev_MKIOS_Channel})/(${today}/${daysInMonth}*SUM(${branchTargetRevenue.target_rev_cvm_outlet}/1000000000)))*100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevBranch.Rev_MKIOS_Channel}) - SUM(${branchTargetRevenue.target_rev_cvm_outlet}/1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevBranch.MoM_MKIOS_Channel}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevBranch.Abs_MKIOS_Channel}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevBranch.YoY_MKIOS_Channel}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevBranch.YTD_MKIOS_Channel}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevBranch.QoQ_MKIOS_Channel}, '%')`.as('qoq_cvm'),
                })
                .from(branchSubquery)
                .leftJoin(summaryRevBranch, eq(branchSubquery.branch, summaryRevBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchSubquery.branch, branchTargetRevenue.branch))
                .groupBy(branchSubquery.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    qoqAll: sql<string>`''`
                })
                .from(feiTargetPuma)

            const subbranchSubquery = db
                .select({ subbranch: territoryArea4.subbranch })
                .from(territoryArea4)
                .where(branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch)
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryBbSubbranch.subbranch,
                    Rev_MKIOS_Channel: max(summaryBbSubbranch.Rev_MKIOS_Channel).as('revMKIOS_Channel'),
                    MoM_MKIOS_Channel: max(summaryBbSubbranch.MoM_MKIOS_Channel).as('momMKIOS_Channel'),
                    Abs_MKIOS_Channel: max(summaryBbSubbranch.Abs_MKIOS_Channel).as('absMKIOS_Channel'),
                    YoY_MKIOS_Channel: max(summaryBbSubbranch.YoY_MKIOS_Channel).as('yoyMKIOS_Channel'),
                    YTD_MKIOS_Channel: max(summaryBbSubbranch.YTD_MKIOS_Channel).as('ytdMKIOS_Channel'),
                    QoQ_MKIOS_Channel: max(summaryBbSubbranch.QoQ_MKIOS_Channel).as('qoqMKIOS_Channel'),
                })
                .from(summaryBbSubbranch)
                .where(and(
                    eq(summaryBbSubbranch.tgl, currDate),
                    eq(summaryBbSubbranch.regional, 'PUMA'),
                    eq(summaryBbSubbranch.type, 'all')
                ))
                .groupBy(summaryBbSubbranch.subbranch)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_rev_cvm_outlet: sum(feiTargetPuma.rev_cvm_outlet).as('target_rev_cvm_outlet')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchSubquery.subbranch,
                    targetAll: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_rev_cvm_outlet}/1000000000), 2)`.as('target_rev_cvm_outlet'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.Rev_MKIOS_Channel}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.Rev_MKIOS_Channel})/SUM(${subbranchTargetRevenue.target_rev_cvm_outlet}/1000000000))*100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.Rev_MKIOS_Channel})/(${today}/${daysInMonth}*SUM(${subbranchTargetRevenue.target_rev_cvm_outlet}/1000000000)))*100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevSubbranch.Rev_MKIOS_Channel}) - SUM(${subbranchTargetRevenue.target_rev_cvm_outlet}/1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevSubbranch.MoM_MKIOS_Channel}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevSubbranch.Abs_MKIOS_Channel}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevSubbranch.YoY_MKIOS_Channel}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevSubbranch.YTD_MKIOS_Channel}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevSubbranch.QoQ_MKIOS_Channel}, '%')`.as('qoq_cvm'),
                })
                .from(subbranchSubquery)
                .leftJoin(summaryRevSubbranch, eq(subbranchSubquery.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchSubquery.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchSubquery.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    qoqAll: sql<string>`''`
                })
                .from(feiTargetPuma)

            const clusterSubquery = db
                .select({ cluster: territoryArea4.cluster })
                .from(territoryArea4)
                .where(branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.cluster)
                .as('a')

            const summaryRevCluster = db
                .select({
                    cluster: summaryBbCluster.cluster,
                    Rev_MKIOS_Channel: max(summaryBbCluster.Rev_MKIOS_Channel).as('revMKIOS_Channel'),
                    MoM_MKIOS_Channel: max(summaryBbCluster.MoM_MKIOS_Channel).as('momMKIOS_Channel'),
                    Abs_MKIOS_Channel: max(summaryBbCluster.Abs_MKIOS_Channel).as('absMKIOS_Channel'),
                    YoY_MKIOS_Channel: max(summaryBbCluster.YoY_MKIOS_Channel).as('yoyMKIOS_Channel'),
                    YTD_MKIOS_Channel: max(summaryBbCluster.YTD_MKIOS_Channel).as('ytdMKIOS_Channel'),
                    QoQ_MKIOS_Channel: max(summaryBbCluster.QoQ_MKIOS_Channel).as('qoqMKIOS_Channel'),
                })
                .from(summaryBbCluster)
                .where(and(
                    eq(summaryBbCluster.tgl, currDate),
                    eq(summaryBbCluster.regional, 'PUMA'),
                    eq(summaryBbCluster.type, 'all')
                ))
                .groupBy(summaryBbCluster.cluster)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_rev_cvm_outlet: sum(feiTargetPuma.rev_cvm_outlet).as('target_rev_cvm_outlet')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterSubquery.cluster,
                    targetAll: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_rev_cvm_outlet}/1000000000), 2)`.as('target_rev_cvm_outlet'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCluster.Rev_MKIOS_Channel}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.Rev_MKIOS_Channel})/SUM(${clusterTargetRevenue.target_rev_cvm_outlet}/1000000000))*100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.Rev_MKIOS_Channel})/(${today}/${daysInMonth}*SUM(${clusterTargetRevenue.target_rev_cvm_outlet}/1000000000)))*100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevCluster.Rev_MKIOS_Channel}) - SUM(${clusterTargetRevenue.target_rev_cvm_outlet}/1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevCluster.MoM_MKIOS_Channel}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevCluster.Abs_MKIOS_Channel}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevCluster.YoY_MKIOS_Channel}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevCluster.YTD_MKIOS_Channel}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevCluster.QoQ_MKIOS_Channel}, '%')`.as('qoq_cvm'),
                })
                .from(clusterSubquery)
                .leftJoin(summaryRevCluster, eq(clusterSubquery.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterSubquery.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterSubquery.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    qoqAll: sql<string>`''`
                })
                .from(feiTargetPuma)

            const kabupatenSubquery = db
                .select({ kabupaten: territoryArea4.kabupaten })
                .from(territoryArea4)
                .where(branch && subbranch && cluster && kabupaten ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                    eq(territoryArea4.kabupaten, kabupaten),
                ) : branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.kabupaten)
                .as('a')

            const summaryRevCity = db
                .select({
                    city: summaryBbCity.city,
                    Rev_MKIOS_Channel: max(summaryBbCity.Rev_MKIOS_Channel).as('revMKIOS_Channel'),
                    MoM_MKIOS_Channel: max(summaryBbCity.MoM_MKIOS_Channel).as('momMKIOS_Channel'),
                    Abs_MKIOS_Channel: max(summaryBbCity.Abs_MKIOS_Channel).as('absMKIOS_Channel'),
                    YoY_MKIOS_Channel: max(summaryBbCity.YoY_MKIOS_Channel).as('yoyMKIOS_Channel'),
                    YTD_MKIOS_Channel: max(summaryBbCity.YTD_MKIOS_Channel).as('ytdMKIOS_Channel'),
                    QoQ_MKIOS_Channel: max(summaryBbCity.QoQ_MKIOS_Channel).as('qoqMKIOS_Channel'),
                })
                .from(summaryBbCity)
                .where(and(
                    eq(summaryBbCity.tgl, currDate),
                    eq(summaryBbCity.regional, 'PUMA'),
                    eq(summaryBbCity.type, 'all')
                ))
                .groupBy(summaryBbCity.city)
                .as('b')

            const cityTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_rev_cvm_outlet: sum(feiTargetPuma.rev_cvm_outlet).as('target_rev_cvm_outlet')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueCity = db
                .select({
                    name: kabupatenSubquery.kabupaten,
                    targetAll: sql<number>`ROUND(SUM(${cityTargetRevenue.target_rev_cvm_outlet}/1000000000), 2)`.as('target_rev_cvm_outlet'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCity.Rev_MKIOS_Channel}), 2)`.as('rev_cvm'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCity.Rev_MKIOS_Channel})/SUM(${cityTargetRevenue.target_rev_cvm_outlet}/1000000000))*100, 2), '%')`.as('ach_target_fm_cvm'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCity.Rev_MKIOS_Channel})/(${today}/${daysInMonth}*SUM(${cityTargetRevenue.target_rev_cvm_outlet}/1000000000)))*100, 2), '%')`.as('drr_cvm'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevCity.Rev_MKIOS_Channel}) - SUM(${cityTargetRevenue.target_rev_cvm_outlet}/1000000000), 0)), 2)`.as('gap_to_target_cvm'),
                    momAll: sql<string>`CONCAT(${summaryRevCity.MoM_MKIOS_Channel}, '%')`.as('mom_cvm'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevCity.Abs_MKIOS_Channel}), 2)`.as('rev_cvm_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevCity.YoY_MKIOS_Channel}, '%')`.as('yoy_cvm'),
                    ytdAll: sql<string>`CONCAT(${summaryRevCity.YTD_MKIOS_Channel}, '%')`.as('ytd_cvm'),
                    qoqAll: sql<string>`CONCAT(${summaryRevCity.QoQ_MKIOS_Channel}, '%')`.as('qoq_cvm'),
                })
                .from(kabupatenSubquery)
                .leftJoin(summaryRevCity, eq(kabupatenSubquery.kabupaten, summaryRevCity.city))
                .leftJoin(cityTargetRevenue, eq(kabupatenSubquery.kabupaten, cityTargetRevenue.kabupaten))
                .groupBy(kabupatenSubquery.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueCity)
            ])

            return c.json(finalDataRevenue, 200)
        })

export default app