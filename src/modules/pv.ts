import { format, getDaysInMonth, subDays } from "date-fns";
import { and, eq, max, sql, sum } from "drizzle-orm";
import { Hono } from "hono";
import { unionAll } from "drizzle-orm/mysql-core";
import { z } from "zod"
import { zValidator } from "@hono/zod-validator";

import { summaryPvConsumeRegional, summaryPvConsumeBranch, summaryPvConsumeSubbranch, summaryPvConsumeCluster, summaryPvConsumeKabupaten, summaryBbRegional, summaryBbBranch, summaryBbSubbranch, summaryBbCluster, summaryBbCity, feiTargetPuma } from "@/db/schema/v_honai_puma";
import { db } from "@/db";
import { territoryArea4 } from "@/db/schema/puma_2025";


const app = new Hono()
    .get('/revenue-redeem-pv', zValidator('query', z.object({ date: z.coerce.date().optional() })),
        async c => {
            const { date } = c.req.valid('query')
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
                    Rev_Voucher_Fisik: max(summaryBbRegional.Rev_Voucher_Fisik).as('revVoucher_Fisik'),
                    MoM_Voucher_Fisik: max(summaryBbRegional.MoM_Voucher_Fisik).as('momVoucher_Fisik'),
                    Abs_Voucher_Fisik: max(summaryBbRegional.Abs_Voucher_Fisik).as('absVoucher_Fisik'),
                    YoY_Voucher_Fisik: max(summaryBbRegional.YoY_Voucher_Fisik).as('yoyVoucher_Fisik'),
                    YTD_Voucher_Fisik: max(summaryBbRegional.YTD_Voucher_Fisik).as('ytdVoucher_Fisik'),
                    QoQ_Voucher_Fisik: max(summaryBbRegional.QoQ_Voucher_Fisik).as('qoqVoucher_Fisik'),
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
                    target_redeem_pv: sum(feiTargetPuma.pv_redeem_prepaid).as('target_redeem_pv')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalSubquery.regional,
                    target_redeem_pv: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_redeem_pv} / 1000000000), 2)`.as('target_redeem_pv'),
                    rev_cvm: sql<number>`ROUND(SUM(${summaryRevRegional.Rev_Voucher_Fisik}), 2)`.as('rev_cvm'),
                    ach_target_fm_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.Rev_Voucher_Fisik}) / SUM(${regionalTargetRevenue.target_redeem_pv} / 1000000000)) * 100, 2), '%')`.as('ach_target_fm_redeem_pv'),
                    drr_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.Rev_Voucher_Fisik}) / (${today} / ${daysInMonth} * SUM(${regionalTargetRevenue.target_redeem_pv} / 1000000000))) * 100, 2), '%')`.as('drr_redeem_pv'),
                    gap_to_target_redeem_pv: sql<number>`ROUND((COALESCE(SUM(${summaryRevRegional.Rev_Voucher_Fisik}) - SUM(${regionalTargetRevenue.target_redeem_pv} / 1000000000), 0)), 2)`.as('gap_to_target_redeem_pv'),
                    mom_cvm: sql<string>`CONCAT(${summaryRevRegional.MoM_Voucher_Fisik}, '%')`.as('mom_cvm'),
                    rev_cvm_absolut: sql<number>`ROUND(SUM(${summaryRevRegional.Abs_Voucher_Fisik}), 2)`.as('rev_cvm_absolut'),
                    yoy_cvm: sql<string>`CONCAT(${summaryRevRegional.YoY_Voucher_Fisik}, '%')`.as('yoy_cvm'),
                    ytd_cvm: sql<string>`CONCAT(${summaryRevRegional.YTD_Voucher_Fisik}, '%')`.as('ytd_cvm'),
                    qoq_cvm: sql<string>`CONCAT(${summaryRevRegional.QoQ_Voucher_Fisik}, '%')`.as('qoq_cvm'),
                })
                .from(regionalSubquery)
                .leftJoin(summaryRevRegional, eq(regionalSubquery.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalSubquery.regional, regionalTargetRevenue.regional))
                .groupBy(regionalSubquery.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    target_redeem_pv: sql<number>`''`,
                    rev_cvm: sql<number>`''`,
                    ach_target_fm_redeem_pv: sql<string>`''`,
                    drr_redeem_pv: sql<string>`''`,
                    gap_to_target_redeem_pv: sql<number>`''`,
                    mom_cvm: sql<string>`''`,
                    rev_cvm_absolut: sql<number>`''`,
                    yoy_cvm: sql<string>`''`,
                    ytd_cvm: sql<string>`''`,
                    qoq_cvm: sql<string>`''`
                })
                .from(feiTargetPuma)

            const branchSubquery = db
                .select({ branch: territoryArea4.branch })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.branch)
                .as('a')

            const summaryRevBranch = db
                .select({
                    branch: summaryBbBranch.branch,
                    Rev_Voucher_Fisik: max(summaryBbBranch.Rev_Voucher_Fisik).as('revVoucher_Fisik'),
                    MoM_Voucher_Fisik: max(summaryBbBranch.MoM_Voucher_Fisik).as('momVoucher_Fisik'),
                    Abs_Voucher_Fisik: max(summaryBbBranch.Abs_Voucher_Fisik).as('absVoucher_Fisik'),
                    YoY_Voucher_Fisik: max(summaryBbBranch.YoY_Voucher_Fisik).as('yoyVoucher_Fisik'),
                    YTD_Voucher_Fisik: max(summaryBbBranch.YTD_Voucher_Fisik).as('ytdVoucher_Fisik'),
                    QoQ_Voucher_Fisik: max(summaryBbBranch.QoQ_Voucher_Fisik).as('qoqVoucher_Fisik'),
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
                    target_redeem_pv: sum(feiTargetPuma.pv_redeem_prepaid).as('target_redeem_pv')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchSubquery.branch,
                    target_redeem_pv: sql<number>`ROUND(SUM(${branchTargetRevenue.target_redeem_pv}/1000000000), 2)`.as('target_redeem_pv'),
                    rev_cvm: sql<number>`ROUND(SUM(${summaryRevBranch.Rev_Voucher_Fisik}), 2)`.as('rev_cvm'),
                    ach_target_fm_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.Rev_Voucher_Fisik})/SUM(${branchTargetRevenue.target_redeem_pv}/1000000000))*100, 2), '%')`.as('ach_target_fm_redeem_pv'),
                    drr_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.Rev_Voucher_Fisik})/(${today}/${daysInMonth}*SUM(${branchTargetRevenue.target_redeem_pv}/1000000000)))*100, 2), '%')`.as('drr_redeem_pv'),
                    gap_to_target_redeem_pv: sql<number>`ROUND((COALESCE(SUM(${summaryRevBranch.Rev_Voucher_Fisik}) - SUM(${branchTargetRevenue.target_redeem_pv}/1000000000), 0)), 2)`.as('gap_to_target_redeem_pv'),
                    mom_cvm: sql<string>`CONCAT(${summaryRevBranch.MoM_Voucher_Fisik}, '%')`.as('mom_cvm'),
                    rev_cvm_absolut: sql<number>`ROUND(SUM(${summaryRevBranch.Abs_Voucher_Fisik}), 2)`.as('rev_cvm_absolut'),
                    yoy_cvm: sql<string>`CONCAT(${summaryRevBranch.YoY_Voucher_Fisik}, '%')`.as('yoy_cvm'),
                    ytd_cvm: sql<string>`CONCAT(${summaryRevBranch.YTD_Voucher_Fisik}, '%')`.as('ytd_cvm'),
                    qoq_cvm: sql<string>`CONCAT(${summaryRevBranch.QoQ_Voucher_Fisik}, '%')`.as('qoq_cvm'),
                })
                .from(branchSubquery)
                .leftJoin(summaryRevBranch, eq(branchSubquery.branch, summaryRevBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchSubquery.branch, branchTargetRevenue.branch))
                .groupBy(branchSubquery.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    target_redeem_pv: sql<number>`''`,
                    rev_cvm: sql<number>`''`,
                    ach_target_fm_redeem_pv: sql<string>`''`,
                    drr_redeem_pv: sql<string>`''`,
                    gap_to_target_redeem_pv: sql<number>`''`,
                    mom_cvm: sql<string>`''`,
                    rev_cvm_absolut: sql<number>`''`,
                    yoy_cvm: sql<string>`''`,
                    ytd_cvm: sql<string>`''`,
                    qoq_cvm: sql<string>`''`
                })
                .from(feiTargetPuma)

            const subbranchSubquery = db
                .select({ subbranch: territoryArea4.subbranch })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryBbSubbranch.subbranch,
                    Rev_Voucher_Fisik: max(summaryBbSubbranch.Rev_Voucher_Fisik).as('revVoucher_Fisik'),
                    MoM_Voucher_Fisik: max(summaryBbSubbranch.MoM_Voucher_Fisik).as('momVoucher_Fisik'),
                    Abs_Voucher_Fisik: max(summaryBbSubbranch.Abs_Voucher_Fisik).as('absVoucher_Fisik'),
                    YoY_Voucher_Fisik: max(summaryBbSubbranch.YoY_Voucher_Fisik).as('yoyVoucher_Fisik'),
                    YTD_Voucher_Fisik: max(summaryBbSubbranch.YTD_Voucher_Fisik).as('ytdVoucher_Fisik'),
                    QoQ_Voucher_Fisik: max(summaryBbSubbranch.QoQ_Voucher_Fisik).as('qoqVoucher_Fisik'),
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
                    target_redeem_pv: sum(feiTargetPuma.pv_redeem_prepaid).as('target_redeem_pv')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchSubquery.subbranch,
                    target_redeem_pv: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_redeem_pv}/1000000000), 2)`.as('target_redeem_pv'),
                    rev_cvm: sql<number>`ROUND(SUM(${summaryRevSubbranch.Rev_Voucher_Fisik}), 2)`.as('rev_cvm'),
                    ach_target_fm_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.Rev_Voucher_Fisik})/SUM(${subbranchTargetRevenue.target_redeem_pv}/1000000000))*100, 2), '%')`.as('ach_target_fm_redeem_pv'),
                    drr_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.Rev_Voucher_Fisik})/(${today}/${daysInMonth}*SUM(${subbranchTargetRevenue.target_redeem_pv}/1000000000)))*100, 2), '%')`.as('drr_redeem_pv'),
                    gap_to_target_redeem_pv: sql<number>`ROUND((COALESCE(SUM(${summaryRevSubbranch.Rev_Voucher_Fisik}) - SUM(${subbranchTargetRevenue.target_redeem_pv}/1000000000), 0)), 2)`.as('gap_to_target_redeem_pv'),
                    mom_cvm: sql<string>`CONCAT(${summaryRevSubbranch.MoM_Voucher_Fisik}, '%')`.as('mom_cvm'),
                    rev_cvm_absolut: sql<number>`ROUND(SUM(${summaryRevSubbranch.Abs_Voucher_Fisik}), 2)`.as('rev_cvm_absolut'),
                    yoy_cvm: sql<string>`CONCAT(${summaryRevSubbranch.YoY_Voucher_Fisik}, '%')`.as('yoy_cvm'),
                    ytd_cvm: sql<string>`CONCAT(${summaryRevSubbranch.YTD_Voucher_Fisik}, '%')`.as('ytd_cvm'),
                    qoq_cvm: sql<string>`CONCAT(${summaryRevSubbranch.QoQ_Voucher_Fisik}, '%')`.as('qoq_cvm'),
                })
                .from(subbranchSubquery)
                .leftJoin(summaryRevSubbranch, eq(subbranchSubquery.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchSubquery.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchSubquery.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    target_redeem_pv: sql<number>`''`,
                    rev_cvm: sql<number>`''`,
                    ach_target_fm_redeem_pv: sql<string>`''`,
                    drr_redeem_pv: sql<string>`''`,
                    gap_to_target_redeem_pv: sql<number>`''`,
                    mom_cvm: sql<string>`''`,
                    rev_cvm_absolut: sql<number>`''`,
                    yoy_cvm: sql<string>`''`,
                    ytd_cvm: sql<string>`''`,
                    qoq_cvm: sql<string>`''`
                })
                .from(feiTargetPuma)

            const clusterSubquery = db
                .select({ cluster: territoryArea4.cluster })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.cluster)
                .as('a')

            const summaryRevCluster = db
                .select({
                    cluster: summaryBbCluster.cluster,
                    Rev_Voucher_Fisik: max(summaryBbCluster.Rev_Voucher_Fisik).as('revVoucher_Fisik'),
                    MoM_Voucher_Fisik: max(summaryBbCluster.MoM_Voucher_Fisik).as('momVoucher_Fisik'),
                    Abs_Voucher_Fisik: max(summaryBbCluster.Abs_Voucher_Fisik).as('absVoucher_Fisik'),
                    YoY_Voucher_Fisik: max(summaryBbCluster.YoY_Voucher_Fisik).as('yoyVoucher_Fisik'),
                    YTD_Voucher_Fisik: max(summaryBbCluster.YTD_Voucher_Fisik).as('ytdVoucher_Fisik'),
                    QoQ_Voucher_Fisik: max(summaryBbCluster.QoQ_Voucher_Fisik).as('qoqVoucher_Fisik'),
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
                    target_redeem_pv: sum(feiTargetPuma.pv_redeem_prepaid).as('target_redeem_pv')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterSubquery.cluster,
                    target_redeem_pv: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_redeem_pv}/1000000000), 2)`.as('target_redeem_pv'),
                    rev_cvm: sql<number>`ROUND(SUM(${summaryRevCluster.Rev_Voucher_Fisik}), 2)`.as('rev_cvm'),
                    ach_target_fm_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.Rev_Voucher_Fisik})/SUM(${clusterTargetRevenue.target_redeem_pv}/1000000000))*100, 2), '%')`.as('ach_target_fm_redeem_pv'),
                    drr_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.Rev_Voucher_Fisik})/(${today}/${daysInMonth}*SUM(${clusterTargetRevenue.target_redeem_pv}/1000000000)))*100, 2), '%')`.as('drr_redeem_pv'),
                    gap_to_target_redeem_pv: sql<number>`ROUND((COALESCE(SUM(${summaryRevCluster.Rev_Voucher_Fisik}) - SUM(${clusterTargetRevenue.target_redeem_pv}/1000000000), 0)), 2)`.as('gap_to_target_redeem_pv'),
                    mom_cvm: sql<string>`CONCAT(${summaryRevCluster.MoM_Voucher_Fisik}, '%')`.as('mom_cvm'),
                    rev_cvm_absolut: sql<number>`ROUND(SUM(${summaryRevCluster.Abs_Voucher_Fisik}), 2)`.as('rev_cvm_absolut'),
                    yoy_cvm: sql<string>`CONCAT(${summaryRevCluster.YoY_Voucher_Fisik}, '%')`.as('yoy_cvm'),
                    ytd_cvm: sql<string>`CONCAT(${summaryRevCluster.YTD_Voucher_Fisik}, '%')`.as('ytd_cvm'),
                    qoq_cvm: sql<string>`CONCAT(${summaryRevCluster.QoQ_Voucher_Fisik}, '%')`.as('qoq_cvm'),
                })
                .from(clusterSubquery)
                .leftJoin(summaryRevCluster, eq(clusterSubquery.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterSubquery.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterSubquery.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    target_redeem_pv: sql<number>`''`,
                    rev_cvm: sql<number>`''`,
                    ach_target_fm_redeem_pv: sql<string>`''`,
                    drr_redeem_pv: sql<string>`''`,
                    gap_to_target_redeem_pv: sql<number>`''`,
                    mom_cvm: sql<string>`''`,
                    rev_cvm_absolut: sql<number>`''`,
                    yoy_cvm: sql<string>`''`,
                    ytd_cvm: sql<string>`''`,
                    qoq_cvm: sql<string>`''`
                })
                .from(feiTargetPuma)

            const kabupatenSubquery = db
                .select({ kabupaten: territoryArea4.kabupaten })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.kabupaten)
                .as('a')

            const summaryRevCity = db
                .select({
                    city: summaryBbCity.city,
                    Rev_Voucher_Fisik: max(summaryBbCity.Rev_Voucher_Fisik).as('revVoucher_Fisik'),
                    MoM_Voucher_Fisik: max(summaryBbCity.MoM_Voucher_Fisik).as('momVoucher_Fisik'),
                    Abs_Voucher_Fisik: max(summaryBbCity.Abs_Voucher_Fisik).as('absVoucher_Fisik'),
                    YoY_Voucher_Fisik: max(summaryBbCity.YoY_Voucher_Fisik).as('yoyVoucher_Fisik'),
                    YTD_Voucher_Fisik: max(summaryBbCity.YTD_Voucher_Fisik).as('ytdVoucher_Fisik'),
                    QoQ_Voucher_Fisik: max(summaryBbCity.QoQ_Voucher_Fisik).as('qoqVoucher_Fisik'),
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
                    target_redeem_pv: sum(feiTargetPuma.pv_redeem_prepaid).as('target_redeem_pv')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, currDateYYMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueCity = db
                .select({
                    name: kabupatenSubquery.kabupaten,
                    target_redeem_pv: sql<number>`ROUND(SUM(${cityTargetRevenue.target_redeem_pv}/1000000000), 2)`.as('target_redeem_pv'),
                    rev_cvm: sql<number>`ROUND(SUM(${summaryRevCity.Rev_Voucher_Fisik}), 2)`.as('rev_cvm'),
                    ach_target_fm_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevCity.Rev_Voucher_Fisik})/SUM(${cityTargetRevenue.target_redeem_pv}/1000000000))*100, 2), '%')`.as('ach_target_fm_redeem_pv'),
                    drr_redeem_pv: sql<string>`CONCAT(ROUND((SUM(${summaryRevCity.Rev_Voucher_Fisik})/(${today}/${daysInMonth}*SUM(${cityTargetRevenue.target_redeem_pv}/1000000000)))*100, 2), '%')`.as('drr_redeem_pv'),
                    gap_to_target_redeem_pv: sql<number>`ROUND((COALESCE(SUM(${summaryRevCity.Rev_Voucher_Fisik}) - SUM(${cityTargetRevenue.target_redeem_pv}/1000000000), 0)), 2)`.as('gap_to_target_redeem_pv'),
                    mom_cvm: sql<string>`CONCAT(${summaryRevCity.MoM_Voucher_Fisik}, '%')`.as('mom_cvm'),
                    rev_cvm_absolut: sql<number>`ROUND(SUM(${summaryRevCity.Abs_Voucher_Fisik}), 2)`.as('rev_cvm_absolut'),
                    yoy_cvm: sql<string>`CONCAT(${summaryRevCity.YoY_Voucher_Fisik}, '%')`.as('yoy_cvm'),
                    ytd_cvm: sql<string>`CONCAT(${summaryRevCity.YTD_Voucher_Fisik}, '%')`.as('ytd_cvm'),
                    qoq_cvm: sql<string>`CONCAT(${summaryRevCity.QoQ_Voucher_Fisik}, '%')`.as('qoq_cvm'),
                })
                .from(kabupatenSubquery)
                .leftJoin(summaryRevCity, eq(kabupatenSubquery.kabupaten, summaryRevCity.city))
                .leftJoin(cityTargetRevenue, eq(kabupatenSubquery.kabupaten, cityTargetRevenue.kabupaten))
                .groupBy(kabupatenSubquery.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueCity)
            ])

            return c.json({ data: finalDataRevenue }, 200)
        })
    .get('/redeem-pv', zValidator('query', z.object({ date: z.coerce.date().optional() })),
        async c => {
            const { date } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 3)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const currMonth = format(selectedDate, 'MM')
            const currYear = format(selectedDate, 'yyyy')
            const daysInMonth = getDaysInMonth(selectedDate)
            const yyyyMM = format(selectedDate, 'yyyyMM')

            const regionalSubquery = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select({
                    regional: summaryPvConsumeRegional.regional,
                    currRev: max(summaryPvConsumeRegional.cur_rev).as('currRev'),
                    revMom: max(summaryPvConsumeRegional.rev_mom).as('revMom'),
                    curTrx: max(summaryPvConsumeRegional.cur_trx).as('curTrx'),
                    trxMom: max(summaryPvConsumeRegional.trx_mom).as('trxMom'),
                })
                .from(summaryPvConsumeRegional)
                .where(and(
                    eq(summaryPvConsumeRegional.tgl, currDate),
                    eq(summaryPvConsumeRegional.regional, 'MALUKU DAN PAPUA'),
                    eq(summaryPvConsumeRegional.denom, 'all')
                ))
                .groupBy(summaryPvConsumeRegional.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    targetPvPrepaid: sum(feiTargetPuma.pv_redeem_prepaid).as('target_pv_prepaid')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalSubquery.regional,
                    targetAll: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetPvPrepaid}),2)`.as('target_all'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevRegional.currRev}),2)`.as('rev_all'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.currRev})/SUM(${regionalTargetRevenue.targetPvPrepaid}))*100,2),'%')`.as('ach_target_fm_all'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.currRev})/(7/${daysInMonth}*(SUM(${regionalTargetRevenue.targetPvPrepaid}))))*100,2),'%')`.as('drr_all'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevRegional.currRev}) - SUM(${regionalTargetRevenue.targetPvPrepaid}),0)),2)`.as('gap_to_target_all'),
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
                })
                .from(feiTargetPuma)

            const branchSubquery = db
                .select({ branch: territoryArea4.branch })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'MALUKU DAN PAPUA'))
                .groupBy(territoryArea4.branch)
                .as('a')

            const summaryRevBranch = db
                .select({
                    branch: summaryPvConsumeBranch.branch,
                    currRev: max(summaryPvConsumeBranch.cur_rev).as('currRev'),
                    revMom: max(summaryPvConsumeBranch.rev_mom).as('revMom'),
                    curTrx: max(summaryPvConsumeBranch.cur_trx).as('curTrx'),
                    trxMom: max(summaryPvConsumeBranch.trx_mom).as('trxMom'),
                })
                .from(summaryPvConsumeBranch)
                .where(and(
                    eq(summaryPvConsumeBranch.tgl, currDate),
                    eq(summaryPvConsumeBranch.regional, 'MALUKU DAN PAPUA'),
                    eq(summaryPvConsumeRegional.denom, 'all')
                ))
                .groupBy(summaryPvConsumeBranch.branch)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    targetPvPrepaid: sum(feiTargetPuma.pv_redeem_prepaid).as('target_pv_prepaid')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchSubquery.branch,
                    targetAll: sql<number>`ROUND(SUM(${branchTargetRevenue.targetPvPrepaid}),2)`.as('target_all'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevBranch.currRev}),2)`.as('rev_all'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.currRev})/SUM(${branchTargetRevenue.targetPvPrepaid}))*100,2),'%')`.as('ach_target_fm_all'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.currRev})/(7/${daysInMonth}*(SUM(${branchTargetRevenue.targetPvPrepaid}))))*100,2),'%')`.as('drr_all'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevBranch.currRev}) - SUM(${branchTargetRevenue.targetPvPrepaid}),0)),2)`.as('gap_to_target_all'),

                })
                .from(branchSubquery)
                .leftJoin(summaryRevBranch, eq(branchSubquery.branch, summaryRevBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchSubquery.branch, branchTargetRevenue.branch))
                .groupBy(branchSubquery.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string>`'SUBBRANCH'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                })
                .from(feiTargetPuma)

            const subbranchSubquery = db
                .select({ subbranch: territoryArea4.subbranch })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'MALUKU DAN PAPUA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryPvConsumeSubbranch.subbranch,
                    currRev: max(summaryPvConsumeSubbranch.cur_rev).as('currRev'),
                    revMom: max(summaryPvConsumeSubbranch.rev_mom).as('revMom'),
                    curTrx: max(summaryPvConsumeSubbranch.cur_trx).as('curTrx'),
                    trxMom: max(summaryPvConsumeSubbranch.trx_mom).as('trxMom'),
                })
                .from(summaryPvConsumeSubbranch)
                .where(and(
                    eq(summaryPvConsumeSubbranch.tgl, currDate),
                    eq(summaryPvConsumeSubbranch.regional, 'MALUKU DAN PAPUA'),
                    eq(summaryPvConsumeSubbranch.denom, 'all')
                ))
                .groupBy(summaryPvConsumeSubbranch.subbranch)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    targetPvPrepaid: sum(feiTargetPuma.pv_redeem_prepaid).as('target_pv_prepaid')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchSubquery.subbranch,
                    targetAll: sql<number>`ROUND(SUM(${subbranchTargetRevenue.targetPvPrepaid}),2)`.as('target_all'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.currRev}),2)`.as('rev_all'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.currRev})/SUM(${subbranchTargetRevenue.targetPvPrepaid}))*100,2),'%')`.as('ach_target_fm_all'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.currRev})/(7/${daysInMonth}*(SUM(${subbranchTargetRevenue.targetPvPrepaid}))))*100,2),'%')`.as('drr_all'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevSubbranch.currRev}) - SUM(${subbranchTargetRevenue.targetPvPrepaid}),0)),2)`.as('gap_to_target_all'),
                })
                .from(subbranchSubquery)
                .leftJoin(summaryRevSubbranch, eq(subbranchSubquery.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchSubquery.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchSubquery.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string>`'CLUSTER'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                })
                .from(feiTargetPuma)

            const clusterSubquery = db
                .select({ cluster: territoryArea4.cluster })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'MALUKU DAN PAPUA'))
                .groupBy(territoryArea4.cluster)
                .as('a')

            const summaryRevCluster = db
                .select({
                    cluster: summaryPvConsumeCluster.cluster,
                    currRev: max(summaryPvConsumeCluster.cur_rev).as('currRev'),
                    revMom: max(summaryPvConsumeCluster.rev_mom).as('revMom'),
                    curTrx: max(summaryPvConsumeCluster.cur_trx).as('curTrx'),
                    trxMom: max(summaryPvConsumeCluster.trx_mom).as('trxMom'),
                })
                .from(summaryPvConsumeCluster)
                .where(and(
                    eq(summaryPvConsumeCluster.tgl, currDate),
                    eq(summaryPvConsumeCluster.regional, 'MALUKU DAN PAPUA'),
                    eq(summaryPvConsumeCluster.denom, 'all')
                ))
                .groupBy(summaryPvConsumeCluster.cluster)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    targetPvPrepaid: sum(feiTargetPuma.pv_redeem_prepaid).as('target_pv_prepaid')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterSubquery.cluster,
                    targetAll: sql<number>`ROUND(SUM(${clusterTargetRevenue.targetPvPrepaid}),2)`.as('target_all'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCluster.currRev}),2)`.as('rev_all'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.currRev})/SUM(${clusterTargetRevenue.targetPvPrepaid}))*100,2),'%')`.as('ach_target_fm_all'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.currRev})/(7/${daysInMonth}*(SUM(${clusterTargetRevenue.targetPvPrepaid}))))*100,2),'%')`.as('drr_all'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevCluster.currRev}) - SUM(${clusterTargetRevenue.targetPvPrepaid}),0)),2)`.as('gap_to_target_all'),

                })
                .from(clusterSubquery)
                .leftJoin(summaryRevCluster, eq(clusterSubquery.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterSubquery.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterSubquery.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string>`'KABUPATEN'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                })
                .from(feiTargetPuma)

            const kabupatenSubquery = db
                .select({ kabupaten: territoryArea4.kabupaten })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'MALUKU DAN PAPUA'))
                .groupBy(territoryArea4.kabupaten)
                .as('a')

            const summaryRevKabupaten = db
                .select({
                    kabupaten: summaryPvConsumeKabupaten.city,
                    currRev: max(summaryPvConsumeKabupaten.cur_rev).as('currRev'),
                    revMom: max(summaryPvConsumeKabupaten.rev_mom).as('revMom'),
                    curTrx: max(summaryPvConsumeKabupaten.cur_trx).as('curTrx'),
                    trxMom: max(summaryPvConsumeKabupaten.trx_mom).as('trxMom'),
                })
                .from(summaryPvConsumeKabupaten)
                .where(and(
                    eq(summaryPvConsumeKabupaten.tgl, currDate),
                    eq(summaryPvConsumeKabupaten.regional, 'MALUKU DAN PAPUA'),
                    eq(summaryPvConsumeKabupaten.denom, 'all')
                ))
                .groupBy(summaryPvConsumeKabupaten.city)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    targetPvPrepaid: sum(feiTargetPuma.pv_redeem_prepaid).as('target_pv_prepaid')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueKabupaten = db
                .select({
                    name: kabupatenSubquery.kabupaten,
                    targetAll: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.targetPvPrepaid}),2)`.as('target_all'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.currRev}),2)`.as('rev_all'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.currRev})/SUM(${kabupatenTargetRevenue.targetPvPrepaid}))*100,2),'%')`.as('ach_target_fm_all'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.currRev})/(7/${daysInMonth}*(SUM(${kabupatenTargetRevenue.targetPvPrepaid}))))*100,2),'%')`.as('drr_all'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevKabupaten.currRev}) - SUM(${kabupatenTargetRevenue.targetPvPrepaid}),0)),2)`.as('gap_to_target_all'),
                })
                .from(kabupatenSubquery)
                .leftJoin(summaryRevKabupaten, eq(kabupatenSubquery.kabupaten, summaryRevKabupaten.kabupaten))
                .leftJoin(kabupatenTargetRevenue, eq(kabupatenSubquery.kabupaten, kabupatenTargetRevenue.kabupaten))
                .groupBy(kabupatenSubquery.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json({ data: finalDataRevenue }, 200)
        })

export default app