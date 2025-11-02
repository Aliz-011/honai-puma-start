import { Hono } from "hono";
import { z } from 'zod'
import { and, eq, sql, sum } from "drizzle-orm";
import { subMonths, subDays, format, subYears, endOfMonth, getDaysInMonth } from 'date-fns'
import { zValidator } from "@hono/zod-validator";

import { db } from "@/db";
import { territoryArea4 } from "@/db/schema/puma_2025";
import { summarySoAllRegional, summarySoAllBranch, summarySoAllSubbranch, summarySoAllCluster, summarySoAllKabupaten, feiTargetPuma } from "@/db/schema/v_honai_puma";
import { index, unionAll } from "drizzle-orm/mysql-core";

const app = new Hono()
    .get('/target-so',
        zValidator('query', z.object({ date: z.coerce.date().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 3)

            const currMonth = format(selectedDate, 'MM')
            const currYear = format(selectedDate, 'yyyy')
            const latestMonth = parseInt(format(selectedDate, 'M'), 10)
            const isPrevMonthLastYear = currMonth === '01'
            const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(selectedDate, 1), 'MM')
            const prevMonthYear = isPrevMonthLastYear ? format(subYears(selectedDate, 1), 'yyyy') : format(selectedDate, 'yyyy')
            const prevYear = format(subYears(selectedDate, 1), 'yyyy')

            // VARIABLE TANGGAL
            // Get the last day of the selected month
            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
            const endOfPrevMonth = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1);
            const endOfPrevYearSameMonth = isEndOfMonth ? endOfMonth(subYears(selectedDate, 1)) : subYears(selectedDate, 1);

            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const currDate = format(endOfCurrMonth, 'yyyy-MM-dd');
            const prevDate = format(endOfPrevMonth, 'yyyy-MM-dd');
            const prevYearCurrDate = format(endOfPrevYearSameMonth, 'yyyy-MM-dd');
            const yyyyMM = format(selectedDate, 'yyyyMM')

            const currJanuaryFirst = `${currYear}-01-01`
            const prevJanuaryFirst = `${prevYear}-01-01`

            const regionalSubquery = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select({
                    regional: summarySoAllRegional.regional,
                    trx_total_mom: summarySoAllRegional.trx_total_mom,
                    trx_all_m: sql<number>`SUM(CASE WHEN ${summarySoAllRegional.tgl} = ${currDate} THEN ${summarySoAllRegional.trx_all_m} ELSE 0 END)`.as('trx_all_m'),
                    trx_all_m1: sql<number>`SUM(CASE WHEN ${summarySoAllRegional.tgl} = ${prevDate} THEN ${summarySoAllRegional.trx_all_m} ELSE 0 END)`.as('trx_all_m1'),
                    trx_all_m12: sql<number>`SUM(CASE WHEN ${summarySoAllRegional.tgl} = ${prevYearCurrDate} THEN ${summarySoAllRegional.trx_all_m} ELSE 0 END)`.as('trx_all_m12'),
                    trx_all_ytd: sql<number>`SUM(CASE WHEN ${summarySoAllRegional.tgl} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN ${summarySoAllRegional.trx_all_m} ELSE 0 END)`.as('trx_all_ytd'),
                    trx_all_ytd1: sql<number>`SUM(CASE WHEN ${summarySoAllRegional.tgl} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN ${summarySoAllRegional.trx_all_m} ELSE 0 END)`.as('trx_all_ytd1'),
                })
                .from(summarySoAllRegional, {
                    useIndex: [
                        index('tgl').on(summarySoAllRegional.tgl),
                        index('regional').on(summarySoAllRegional.regional)
                    ]
                })
                .where(eq(summarySoAllRegional.regional, 'PUMA'))
                .groupBy(summarySoAllRegional.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_so: sum(feiTargetPuma.so).as('target_so')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalSubquery.regional,
                    target_so: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_so}),2)`.as('target_so'),
                    so: sql<number>`ROUND(SUM(${summaryRevRegional.trx_all_m}),2)`.as('so'),
                    ach_target_fm_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.trx_all_m})/SUM(${regionalTargetRevenue.target_so}))*100,2),'%')`.as('ach_target_fm_so'),
                    drr_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.trx_all_m})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.target_so}))))*100,2),'%')`.as('drr_so'),
                    gap_to_target_so: sql<number>`ROUND((COALESCE(SUM(${summaryRevRegional.trx_all_m}) - SUM(${regionalTargetRevenue.target_so}),0)),2)`.as('gap_to_target_so'),
                    mom_so: sql<string>`CONCAT(${summaryRevRegional.trx_total_mom}, '%')`.as('mom_so'),
                    so_absolut: sql<number>`ROUND(SUM(${summaryRevRegional.trx_all_m} - ${summaryRevRegional.trx_all_m1}),2)`.as('so_absolut'),
                    yoy_so: sql<string>`CONCAT(SUM(${summaryRevRegional.trx_all_m} - ${summaryRevRegional.trx_all_m12})/${summaryRevRegional.trx_all_m12}*100, '%')`.as('yoy_so'),
                    ytd_so: sql<string>`CONCAT(SUM(${summaryRevRegional.trx_all_ytd} - ${summaryRevRegional.trx_all_ytd1})/${summaryRevRegional.trx_all_ytd1}*100, '%')`.as('ytd_so')
                })
                .from(regionalSubquery)
                .leftJoin(summaryRevRegional, eq(regionalSubquery.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalSubquery.regional, regionalTargetRevenue.regional))
                .groupBy(regionalSubquery.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    target_so: sql<number>`''`,
                    so: sql<number>`''`,
                    ach_target_fm_so: sql<string>`''`,
                    drr_so: sql<string>`''`,
                    gap_to_target_so: sql<number>`''`,
                    mom_so: sql<string>`''`,
                    so_absolut: sql<number>`''`,
                    yoy_so: sql<string>`''`,
                    ytd_so: sql<string>`''`,
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
                    branch: summarySoAllBranch.branch,
                    trx_total_mom: summarySoAllBranch.trx_total_mom,
                    trx_all_m: sql<number>`SUM(CASE WHEN ${summarySoAllBranch.tgl} = ${currDate} THEN ${summarySoAllBranch.trx_all_m} ELSE 0 END)`.as('trx_all_m'),
                    trx_all_m1: sql<number>`SUM(CASE WHEN ${summarySoAllBranch.tgl} = ${prevDate} THEN ${summarySoAllBranch.trx_all_m} ELSE 0 END)`.as('trx_all_m1'),
                    trx_all_m12: sql<number>`SUM(CASE WHEN ${summarySoAllBranch.tgl} = ${prevYearCurrDate} THEN ${summarySoAllBranch.trx_all_m} ELSE 0 END)`.as('trx_all_m12'),
                    trx_all_ytd: sql<number>`SUM(CASE WHEN ${summarySoAllBranch.tgl} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN ${summarySoAllBranch.trx_all_m} ELSE 0 END)`.as('trx_all_ytd'),
                    trx_all_ytd1: sql<number>`SUM(CASE WHEN ${summarySoAllBranch.tgl} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN ${summarySoAllBranch.trx_all_m} ELSE 0 END)`.as('trx_all_ytd1'),
                })
                .from(summarySoAllBranch, {
                    useIndex: [
                        index('tgl').on(summarySoAllBranch.tgl),
                        index('regional').on(summarySoAllBranch.regional)
                    ]
                })
                .where(eq(summarySoAllBranch.regional, 'PUMA'))
                .groupBy(summarySoAllBranch.branch)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_so: sum(feiTargetPuma.so).as('target_so')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchSubquery.branch,
                    target_so: sql<number>`ROUND(SUM(${branchTargetRevenue.target_so}),2)`.as('target_so'),
                    so: sql<number>`ROUND(SUM(${summaryRevBranch.trx_all_m}),2)`.as('so'),
                    ach_target_fm_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.trx_all_m})/SUM(${branchTargetRevenue.target_so}))*100,2),'%')`.as('ach_target_fm_so'),
                    drr_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.trx_all_m})/(${today}/${daysInMonth}*(SUM(${branchTargetRevenue.target_so}))))*100,2),'%')`.as('drr_so'),
                    gap_to_target_so: sql<number>`ROUND((COALESCE(SUM(${summaryRevBranch.trx_all_m}) - SUM(${branchTargetRevenue.target_so}),0)),2)`.as('gap_to_target_so'),
                    mom_so: sql<string>`CONCAT(${summaryRevBranch.trx_total_mom}, '%')`.as('mom_so'),
                    so_absolut: sql<number>`ROUND(SUM(${summaryRevBranch.trx_all_m} - ${summaryRevBranch.trx_all_m1}),2)`.as('so_absolut'),
                    yoy_so: sql<string>`CONCAT(SUM(${summaryRevBranch.trx_all_m} - ${summaryRevBranch.trx_all_m12})/${summaryRevBranch.trx_all_m12}*100, '%')`.as('yoy_so'),
                    ytd_so: sql<string>`CONCAT(SUM(${summaryRevBranch.trx_all_ytd} - ${summaryRevBranch.trx_all_ytd1})/${summaryRevBranch.trx_all_ytd1}*100, '%')`.as('ytd_so')
                })
                .from(branchSubquery)
                .leftJoin(summaryRevBranch, eq(branchSubquery.branch, summaryRevBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchSubquery.branch, branchTargetRevenue.branch))
                .groupBy(branchSubquery.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    target_so: sql<number>`''`,
                    so: sql<number>`''`,
                    ach_target_fm_so: sql<string>`''`,
                    drr_so: sql<string>`''`,
                    gap_to_target_so: sql<number>`''`,
                    mom_so: sql<string>`''`,
                    so_absolut: sql<number>`''`,
                    yoy_so: sql<string>`''`,
                    ytd_so: sql<string>`''`,
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
                    subbranch: summarySoAllSubbranch.subbranch,
                    trx_total_mom: summarySoAllSubbranch.trx_total_mom,
                    trx_all_m: sql<number>`SUM(CASE WHEN ${summarySoAllSubbranch.tgl} = ${currDate} THEN ${summarySoAllSubbranch.trx_all_m} ELSE 0 END)`.as('trx_all_m'),
                    trx_all_m1: sql<number>`SUM(CASE WHEN ${summarySoAllSubbranch.tgl} = ${prevDate} THEN ${summarySoAllSubbranch.trx_all_m} ELSE 0 END)`.as('trx_all_m1'),
                    trx_all_m12: sql<number>`SUM(CASE WHEN ${summarySoAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summarySoAllSubbranch.trx_all_m} ELSE 0 END)`.as('trx_all_m12'),
                    trx_all_ytd: sql<number>`SUM(CASE WHEN ${summarySoAllSubbranch.tgl} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN ${summarySoAllSubbranch.trx_all_m} ELSE 0 END)`.as('trx_all_ytd'),
                    trx_all_ytd1: sql<number>`SUM(CASE WHEN ${summarySoAllSubbranch.tgl} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN ${summarySoAllSubbranch.trx_all_m} ELSE 0 END)`.as('trx_all_ytd1'),
                })
                .from(summarySoAllSubbranch, {
                    useIndex: [
                        index('tgl').on(summarySoAllSubbranch.tgl),
                        index('regional').on(summarySoAllSubbranch.regional)
                    ]
                })
                .where(eq(summarySoAllSubbranch.regional, 'PUMA'))
                .groupBy(summarySoAllSubbranch.subbranch)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_so: sum(feiTargetPuma.so).as('target_so')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchSubquery.subbranch,
                    target_so: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_so}),2)`.as('target_so'),
                    so: sql<number>`ROUND(SUM(${summaryRevSubbranch.trx_all_m}),2)`.as('so'),
                    ach_target_fm_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.trx_all_m})/SUM(${subbranchTargetRevenue.target_so}))*100,2),'%')`.as('ach_target_fm_so'),
                    drr_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.trx_all_m})/(${today}/${daysInMonth}*(SUM(${subbranchTargetRevenue.target_so}))))*100,2),'%')`.as('drr_so'),
                    gap_to_target_so: sql<number>`ROUND((COALESCE(SUM(${summaryRevSubbranch.trx_all_m}) - SUM(${subbranchTargetRevenue.target_so}),0)),2)`.as('gap_to_target_so'),
                    mom_so: sql<string>`CONCAT(${summaryRevSubbranch.trx_total_mom}, '%')`.as('mom_so'),
                    so_absolut: sql<number>`ROUND(SUM(${summaryRevSubbranch.trx_all_m} - ${summaryRevSubbranch.trx_all_m1}),2)`.as('so_absolut'),
                    yoy_so: sql<string>`CONCAT(SUM(${summaryRevSubbranch.trx_all_m} - ${summaryRevSubbranch.trx_all_m12})/${summaryRevSubbranch.trx_all_m12}*100, '%')`.as('yoy_so'),
                    ytd_so: sql<string>`CONCAT(SUM(${summaryRevSubbranch.trx_all_ytd} - ${summaryRevSubbranch.trx_all_ytd1})/${summaryRevSubbranch.trx_all_ytd1}*100, '%')`.as('ytd_so')
                })
                .from(subbranchSubquery)
                .leftJoin(summaryRevSubbranch, eq(subbranchSubquery.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchSubquery.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchSubquery.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    target_so: sql<number>`''`,
                    so: sql<number>`''`,
                    ach_target_fm_so: sql<string>`''`,
                    drr_so: sql<string>`''`,
                    gap_to_target_so: sql<number>`''`,
                    mom_so: sql<string>`''`,
                    so_absolut: sql<number>`''`,
                    yoy_so: sql<string>`''`,
                    ytd_so: sql<string>`''`,
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
                    cluster: summarySoAllCluster.cluster,
                    trx_total_mom: summarySoAllCluster.trx_total_mom,
                    trx_all_m: sql<number>`SUM(CASE WHEN ${summarySoAllCluster.tgl} = ${currDate} THEN ${summarySoAllCluster.trx_all_m} ELSE 0 END)`.as('trx_all_m'),
                    trx_all_m1: sql<number>`SUM(CASE WHEN ${summarySoAllCluster.tgl} = ${prevDate} THEN ${summarySoAllCluster.trx_all_m} ELSE 0 END)`.as('trx_all_m1'),
                    trx_all_m12: sql<number>`SUM(CASE WHEN ${summarySoAllCluster.tgl} = ${prevYearCurrDate} THEN ${summarySoAllCluster.trx_all_m} ELSE 0 END)`.as('trx_all_m12'),
                    trx_all_ytd: sql<number>`SUM(CASE WHEN ${summarySoAllCluster.tgl} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN ${summarySoAllCluster.trx_all_m} ELSE 0 END)`.as('trx_all_ytd'),
                    trx_all_ytd1: sql<number>`SUM(CASE WHEN ${summarySoAllCluster.tgl} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN ${summarySoAllCluster.trx_all_m} ELSE 0 END)`.as('trx_all_ytd1'),
                })
                .from(summarySoAllCluster, {
                    useIndex: [
                        index('tgl').on(summarySoAllCluster.tgl),
                        index('regional').on(summarySoAllCluster.regional)
                    ]
                })
                .where(eq(summarySoAllCluster.regional, 'PUMA'))
                .groupBy(summarySoAllCluster.cluster)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_so: sum(feiTargetPuma.so).as('target_so')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterSubquery.cluster,
                    target_so: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_so}),2)`.as('target_so'),
                    so: sql<number>`ROUND(SUM(${summaryRevCluster.trx_all_m}),2)`.as('so'),
                    ach_target_fm_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.trx_all_m})/SUM(${clusterTargetRevenue.target_so}))*100,2),'%')`.as('ach_target_fm_so'),
                    drr_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.trx_all_m})/(${today}/${daysInMonth}*(SUM(${clusterTargetRevenue.target_so}))))*100,2),'%')`.as('drr_so'),
                    gap_to_target_so: sql<number>`ROUND((COALESCE(SUM(${summaryRevCluster.trx_all_m}) - SUM(${clusterTargetRevenue.target_so}),0)),2)`.as('gap_to_target_so'),
                    mom_so: sql<string>`CONCAT(${summaryRevCluster.trx_total_mom}, '%')`.as('mom_so'),
                    so_absolut: sql<number>`ROUND(SUM(${summaryRevCluster.trx_all_m} - ${summaryRevCluster.trx_all_m1}),2)`.as('so_absolut'),
                    yoy_so: sql<string>`CONCAT(SUM(${summaryRevCluster.trx_all_m} - ${summaryRevCluster.trx_all_m12})/${summaryRevCluster.trx_all_m12}*100, '%')`.as('yoy_so'),
                    ytd_so: sql<string>`CONCAT(SUM(${summaryRevCluster.trx_all_ytd} - ${summaryRevCluster.trx_all_ytd1})/${summaryRevCluster.trx_all_ytd1}*100, '%')`.as('ytd_so')
                })
                .from(clusterSubquery)
                .leftJoin(summaryRevCluster, eq(clusterSubquery.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterSubquery.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterSubquery.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    target_so: sql<number>`''`,
                    so: sql<number>`''`,
                    ach_target_fm_so: sql<string>`''`,
                    drr_so: sql<string>`''`,
                    gap_to_target_so: sql<number>`''`,
                    mom_so: sql<string>`''`,
                    so_absolut: sql<number>`''`,
                    yoy_so: sql<string>`''`,
                    ytd_so: sql<string>`''`,
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

            const summaryRevKabupaten = db
                .select({
                    kabupaten: summarySoAllKabupaten.kabupaten,
                    trx_total_mom: summarySoAllKabupaten.trx_total_mom,
                    trx_all_m: sql<number>`SUM(CASE WHEN ${summarySoAllKabupaten.tgl} = ${currDate} THEN ${summarySoAllKabupaten.trx_all_m} ELSE 0 END)`.as('trx_all_m'),
                    trx_all_m1: sql<number>`SUM(CASE WHEN ${summarySoAllKabupaten.tgl} = ${prevDate} THEN ${summarySoAllKabupaten.trx_all_m} ELSE 0 END)`.as('trx_all_m1'),
                    trx_all_m12: sql<number>`SUM(CASE WHEN ${summarySoAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summarySoAllKabupaten.trx_all_m} ELSE 0 END)`.as('trx_all_m12'),
                    trx_all_ytd: sql<number>`SUM(CASE WHEN ${summarySoAllKabupaten.tgl} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN ${summarySoAllKabupaten.trx_all_m} ELSE 0 END)`.as('trx_all_ytd'),
                    trx_all_ytd1: sql<number>`SUM(CASE WHEN ${summarySoAllKabupaten.tgl} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN ${summarySoAllKabupaten.trx_all_m} ELSE 0 END)`.as('trx_all_ytd1'),
                })
                .from(summarySoAllKabupaten, {
                    useIndex: [
                        index('tgl').on(summarySoAllKabupaten.tgl),
                        index('regional').on(summarySoAllKabupaten.regional)
                    ]
                })
                .where(eq(summarySoAllKabupaten.regional, 'PUMA'))
                .groupBy(summarySoAllKabupaten.kabupaten)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_so: sum(feiTargetPuma.so).as('target_so')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueKabupaten = db
                .select({
                    name: kabupatenSubquery.kabupaten,
                    target_so: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.target_so}),2)`.as('target_so'),
                    so: sql<number>`ROUND(SUM(${summaryRevKabupaten.trx_all_m}),2)`.as('so'),
                    ach_target_fm_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.trx_all_m})/SUM(${kabupatenTargetRevenue.target_so}))*100,2),'%')`.as('ach_target_fm_so'),
                    drr_so: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.trx_all_m})/(${today}/${daysInMonth}*(SUM(${kabupatenTargetRevenue.target_so}))))*100,2),'%')`.as('drr_so'),
                    gap_to_target_so: sql<number>`ROUND((COALESCE(SUM(${summaryRevKabupaten.trx_all_m}) - SUM(${kabupatenTargetRevenue.target_so}),0)),2)`.as('gap_to_target_so'),
                    mom_so: sql<string>`CONCAT(${summaryRevKabupaten.trx_total_mom}, '%')`.as('mom_so'),
                    so_absolut: sql<number>`ROUND(SUM(${summaryRevKabupaten.trx_all_m} - ${summaryRevKabupaten.trx_all_m1}),2)`.as('so_absolut'),
                    yoy_so: sql<string>`CONCAT(SUM(${summaryRevKabupaten.trx_all_m} - ${summaryRevKabupaten.trx_all_m12})/${summaryRevKabupaten.trx_all_m12}*100, '%')`.as('yoy_so'),
                    ytd_so: sql<string>`CONCAT(SUM(${summaryRevKabupaten.trx_all_ytd} - ${summaryRevKabupaten.trx_all_ytd1})/${summaryRevKabupaten.trx_all_ytd1}*100, '%')`.as('ytd_so')
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