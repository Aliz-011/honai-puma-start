import { format, getDaysInMonth, subDays } from "date-fns";
import { and, eq, sql, sum } from "drizzle-orm";
import { Hono } from "hono";
import { z } from "zod"
import { zValidator } from "@hono/zod-validator";

import { summaryRevAllByLosRegional, summaryRevAllByLosBranch, summaryRevAllByLosSubbranch, summaryRevAllByLosCluster, summaryRevAllByLosKabupaten, summaryRevNsPrabayarKabupaten, feiTargetPuma, summaryTrxNsAllKabupaten, summaryTrxNsPrabayarKabupaten, summaryTrxNsByuKabupaten } from "@/db/schema/v_honai_puma";
import { db } from "@/db";
import { territoryArea4 } from "@/db/schema/puma_2025";
import { index, unionAll } from "drizzle-orm/mysql-core";

const app = new Hono()
    .get('/trx-new-sales', zValidator('query', z.object({ date: z.coerce.date().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalTerritory = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select({
                    regional: summaryTrxNsAllKabupaten.regional,
                    trx_all_m: sum(summaryTrxNsAllKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsAllKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsAllKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsAllKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsAllKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsAllKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsAllKabupaten)
                .where(and(
                    eq(summaryTrxNsAllKabupaten.tgl, currDate),
                    eq(summaryTrxNsAllKabupaten.regional, 'PUMA'),
                ))
                .groupBy(summaryTrxNsAllKabupaten.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_trx_ns: sum(feiTargetPuma.trx_ns).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    targetAll: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevRegional.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.trx_all_m})/(SUM(${regionalTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.trx_all_m})/(${today}/${daysInMonth}*((SUM(${regionalTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevRegional.trx_all_m}) - (SUM(${regionalTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                })
                .from(regionalTerritory)
                .leftJoin(summaryRevRegional, eq(regionalTerritory.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.regional, regionalTargetRevenue.regional))
                .groupBy(regionalTerritory.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`
                })
                .from(feiTargetPuma)

            const branchTerritory = db
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
                    branch: sql<string>`
                    CASE
                        WHEN kabupaten IN (
                            'AMBON',
                            'KOTA AMBON',
                            'MALUKU TENGAH',
                            'SERAM BAGIAN TIMUR',
                            'KEPULAUAN ARU',
                            'KOTA TUAL',
                            'MALUKU BARAT DAYA',
                            'MALUKU TENGGARA',
                            'MALUKU TENGGARA BARAT',
                            'BURU',
                            'BURU SELATAN',
                            'SERAM BAGIAN BARAT',
                            'KEPULAUAN TANIMBAR'
                        ) THEN 'AMBON'
                        WHEN kabupaten IN (
                            'KOTA JAYAPURA',
                            'JAYAPURA',
                            'KEEROM',
                            'MAMBERAMO RAYA',
                            'SARMI',
                            'BIAK',
                            'BIAK NUMFOR',
                            'KEPULAUAN YAPEN',
                            'SUPIORI',
                            'WAROPEN',
                            'JAYAWIJAYA',
                            'LANNY JAYA',
                            'MAMBERAMO TENGAH',
                            'NDUGA',
                            'PEGUNUNGAN BINTANG',
                            'TOLIKARA',
                            'YAHUKIMO',
                            'YALIMO'
                        ) THEN 'JAYAPURA'
                        WHEN kabupaten IN (
                            'MANOKWARI',
                            'FAKFAK',
                            'FAK FAK',
                            'KAIMANA',
                            'MANOKWARI SELATAN',
                            'PEGUNUNGAN ARFAK',
                            'TELUK BINTUNI',
                            'TELUK WONDAMA',
                            'KOTA SORONG',
                            'MAYBRAT',
                            'RAJA AMPAT',
                            'SORONG',
                            'SORONG SELATAN',
                            'TAMBRAUW'
                        ) THEN 'SORONG'
                        WHEN kabupaten IN (
                            'ASMAT',
                            'BOVEN DIGOEL',
                            'MAPPI',
                            'MERAUKE',
                            'INTAN JAYA',
                            'MIMIKA',
                            'PUNCAK',
                            'PUNCAK JAYA',
                            'TIMIKA',
                            'DEIYAI',
                            'DOGIYAI',
                            'NABIRE',
                            'PANIAI'
                        ) THEN 'TIMIKA'
                        ELSE NULL
                    END
                    `.as('branch'),
                    trx_all_m: sum(summaryTrxNsAllKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsAllKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsAllKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsAllKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsAllKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsAllKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsAllKabupaten)
                .where(and(
                    eq(summaryTrxNsAllKabupaten.tgl, currDate),
                    eq(summaryTrxNsAllKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_trx_ns: sum(feiTargetPuma.trx_ns).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchTerritory.branch,
                    targetAll: sql<number>`ROUND(SUM(${branchTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`SUM(${summaryRevBranch.trx_all_m})`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.trx_all_m})/(SUM(${branchTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.trx_all_m})/(${today}/${daysInMonth}*((SUM(${branchTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevBranch.trx_all_m}) - (SUM(${branchTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                })
                .from(branchTerritory)
                .leftJoin(summaryRevBranch, eq(branchTerritory.branch, sql`b.branch`))
                .leftJoin(branchTargetRevenue, eq(branchTerritory.branch, branchTargetRevenue.branch))
                .groupBy(branchTerritory.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`
                })
                .from(feiTargetPuma)

            const subbranchTerritory = db
                .select()
                .from(territoryArea4)
                .where(branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryTrxNsAllKabupaten.subbranch,
                    trx_all_m: sum(summaryTrxNsAllKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsAllKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsAllKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsAllKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsAllKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsAllKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsAllKabupaten)
                .where(and(
                    eq(summaryTrxNsAllKabupaten.tgl, currDate),
                    eq(summaryTrxNsAllKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_trx_ns: sum(feiTargetPuma.trx_ns).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchTerritory.subbranch,
                    targetAll: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.trx_all_m})/(SUM(${subbranchTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.trx_all_m})/(${today}/${daysInMonth}*((SUM(${subbranchTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.trx_all_m}) - (SUM(${subbranchTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                })
                .from(subbranchTerritory)
                .leftJoin(summaryRevSubbranch, eq(subbranchTerritory.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchTerritory.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchTerritory.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`
                })
                .from(feiTargetPuma)

            const clusterTerritory = db
                .select()
                .from(territoryArea4)
                .where(branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster)
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevCluster = db
                .select({
                    cluster: summaryTrxNsAllKabupaten.cluster,
                    trx_all_m: sum(summaryTrxNsAllKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsAllKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsAllKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsAllKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsAllKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsAllKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsAllKabupaten)
                .where(and(
                    eq(summaryTrxNsAllKabupaten.tgl, currDate),
                    eq(summaryTrxNsAllKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_trx_ns: sum(feiTargetPuma.trx_ns).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterTerritory.cluster,
                    targetAll: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCluster.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.trx_all_m})/(SUM(${clusterTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.trx_all_m})/(${today}/${daysInMonth}*((SUM(${clusterTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevCluster.trx_all_m}) - (SUM(${clusterTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                })
                .from(clusterTerritory)
                .leftJoin(summaryRevCluster, eq(clusterTerritory.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterTerritory.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterTerritory.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    targetAll: sql<number>`''`,
                    revAll: sql<number>`''`,
                    achTargetFmAll: sql<string>`''`,
                    drrAll: sql<string>`''`,
                    gapToTargetAll: sql<number>`''`,
                    momAll: sql<string>`''`,
                    yoyAll: sql<string>`''`,
                    ytdAll: sql<string>`''`,
                    revAllAbsolut: sql<number>`''`
                })
                .from(feiTargetPuma)

            const kabupatenTerritory = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                })
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
                    kabupaten: summaryTrxNsAllKabupaten.kabupaten,
                    trx_all_m: sum(summaryTrxNsAllKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsAllKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsAllKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsAllKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsAllKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsAllKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsAllKabupaten)
                .where(and(
                    eq(summaryTrxNsAllKabupaten.tgl, currDate),
                    eq(summaryTrxNsAllKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_trx_ns: sum(feiTargetPuma.trx_ns).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueKabupaten = db
                .select({
                    name: kabupatenTerritory.kabupaten,
                    targetAll: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.trx_all_m})/(SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.trx_all_m})/(${today}/${daysInMonth}*((SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.trx_all_m}) - (SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                })
                .from(kabupatenTerritory)
                .leftJoin(summaryRevKabupaten, eq(kabupatenTerritory.kabupaten, summaryRevKabupaten.kabupaten))
                .leftJoin(kabupatenTargetRevenue, eq(kabupatenTerritory.kabupaten, kabupatenTargetRevenue.kabupaten))
                .groupBy(kabupatenTerritory.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json(finalDataRevenue)
        })
    .get('/trx-new-sales-prabayar', zValidator('query', z.object({ date: z.coerce.date().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalTerritory = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select({
                    regional: summaryTrxNsPrabayarKabupaten.regional,
                    trx_all_m: sum(summaryTrxNsPrabayarKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsPrabayarKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsPrabayarKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsPrabayarKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsPrabayarKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsPrabayarKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsPrabayarKabupaten)
                .where(and(
                    eq(summaryTrxNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryTrxNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(summaryTrxNsPrabayarKabupaten.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_prepaid).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    targetAll: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevRegional.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.trx_all_m})/(SUM(${regionalTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.trx_all_m})/(${today}/${daysInMonth}*((SUM(${regionalTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevRegional.trx_all_m}) - (SUM(${regionalTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(regionalTerritory)
                .leftJoin(summaryRevRegional, eq(regionalTerritory.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.regional, regionalTargetRevenue.regional))
                .groupBy(regionalTerritory.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const branchTerritory = db
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
                    branch: sql<string>`
                    CASE
                        WHEN kabupaten IN (
                            'AMBON',
                            'KOTA AMBON',
                            'MALUKU TENGAH',
                            'SERAM BAGIAN TIMUR',
                            'KEPULAUAN ARU',
                            'KOTA TUAL',
                            'MALUKU BARAT DAYA',
                            'MALUKU TENGGARA',
                            'MALUKU TENGGARA BARAT',
                            'BURU',
                            'BURU SELATAN',
                            'SERAM BAGIAN BARAT',
                            'KEPULAUAN TANIMBAR'
                        ) THEN 'AMBON'
                        WHEN kabupaten IN (
                            'KOTA JAYAPURA',
                            'JAYAPURA',
                            'KEEROM',
                            'MAMBERAMO RAYA',
                            'SARMI',
                            'BIAK',
                            'BIAK NUMFOR',
                            'KEPULAUAN YAPEN',
                            'SUPIORI',
                            'WAROPEN',
                            'JAYAWIJAYA',
                            'LANNY JAYA',
                            'MAMBERAMO TENGAH',
                            'NDUGA',
                            'PEGUNUNGAN BINTANG',
                            'TOLIKARA',
                            'YAHUKIMO',
                            'YALIMO'
                        ) THEN 'JAYAPURA'
                        WHEN kabupaten IN (
                            'MANOKWARI',
                            'FAKFAK',
                            'FAK FAK',
                            'KAIMANA',
                            'MANOKWARI SELATAN',
                            'PEGUNUNGAN ARFAK',
                            'TELUK BINTUNI',
                            'TELUK WONDAMA',
                            'KOTA SORONG',
                            'MAYBRAT',
                            'RAJA AMPAT',
                            'SORONG',
                            'SORONG SELATAN',
                            'TAMBRAUW'
                        ) THEN 'SORONG'
                        WHEN kabupaten IN (
                            'ASMAT',
                            'BOVEN DIGOEL',
                            'MAPPI',
                            'MERAUKE',
                            'INTAN JAYA',
                            'MIMIKA',
                            'PUNCAK',
                            'PUNCAK JAYA',
                            'TIMIKA',
                            'DEIYAI',
                            'DOGIYAI',
                            'NABIRE',
                            'PANIAI'
                        ) THEN 'TIMIKA'
                        ELSE NULL
                    END
                    `.as('branch'),
                    trx_all_m: sum(summaryTrxNsPrabayarKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsPrabayarKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsPrabayarKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsPrabayarKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsPrabayarKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsPrabayarKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsPrabayarKabupaten)
                .where(and(
                    eq(summaryTrxNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryTrxNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_prepaid).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchTerritory.branch,
                    targetAll: sql<number>`ROUND(SUM(${branchTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`SUM(${summaryRevBranch.trx_all_m})`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.trx_all_m})/(SUM(${branchTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.trx_all_m})/(${today}/${daysInMonth}*((SUM(${branchTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevBranch.trx_all_m}) - (SUM(${branchTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(branchTerritory)
                .leftJoin(summaryRevBranch, eq(branchTerritory.branch, sql`b.branch`))
                .leftJoin(branchTargetRevenue, eq(branchTerritory.branch, branchTargetRevenue.branch))
                .groupBy(branchTerritory.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const subbranchTerritory = db
                .select()
                .from(territoryArea4)
                .where(branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryTrxNsPrabayarKabupaten.subbranch,
                    trx_all_m: sum(summaryTrxNsPrabayarKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsPrabayarKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsPrabayarKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsPrabayarKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsPrabayarKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsPrabayarKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsPrabayarKabupaten)
                .where(and(
                    eq(summaryTrxNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryTrxNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_prepaid).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchTerritory.subbranch,
                    targetAll: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.trx_all_m})/(SUM(${subbranchTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.trx_all_m})/(${today}/${daysInMonth}*((SUM(${subbranchTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.trx_all_m}) - (SUM(${subbranchTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(subbranchTerritory)
                .leftJoin(summaryRevSubbranch, eq(subbranchTerritory.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchTerritory.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchTerritory.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const clusterTerritory = db
                .select()
                .from(territoryArea4)
                .where(branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster)
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevCluster = db
                .select({
                    cluster: summaryTrxNsPrabayarKabupaten.cluster,
                    trx_all_m: sum(summaryTrxNsPrabayarKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsPrabayarKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsPrabayarKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsPrabayarKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsPrabayarKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsPrabayarKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsPrabayarKabupaten)
                .where(and(
                    eq(summaryTrxNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryTrxNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_prepaid).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterTerritory.cluster,
                    targetAll: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCluster.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.trx_all_m})/(SUM(${clusterTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.trx_all_m})/(${today}/${daysInMonth}*((SUM(${clusterTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevCluster.trx_all_m}) - (SUM(${clusterTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(clusterTerritory)
                .leftJoin(summaryRevCluster, eq(clusterTerritory.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterTerritory.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterTerritory.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const kabupatenTerritory = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                })
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
                    kabupaten: summaryTrxNsPrabayarKabupaten.kabupaten,
                    trx_all_m: sum(summaryTrxNsPrabayarKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsPrabayarKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsPrabayarKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsPrabayarKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsPrabayarKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsPrabayarKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsPrabayarKabupaten)
                .where(and(
                    eq(summaryTrxNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryTrxNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_prepaid).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueKabupaten = db
                .select({
                    name: kabupatenTerritory.kabupaten,
                    targetAll: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.trx_all_m})/(SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.trx_all_m})/(${today}/${daysInMonth}*((SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.trx_all_m}) - (SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(kabupatenTerritory)
                .leftJoin(summaryRevKabupaten, eq(kabupatenTerritory.kabupaten, summaryRevKabupaten.kabupaten))
                .leftJoin(kabupatenTargetRevenue, eq(kabupatenTerritory.kabupaten, kabupatenTargetRevenue.kabupaten))
                .groupBy(kabupatenTerritory.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json(finalDataRevenue)
        })
    .get('/trx-new-sales-byu', zValidator('query', z.object({ date: z.coerce.date().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalTerritory = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select({
                    regional: summaryTrxNsByuKabupaten.regional,
                    trx_all_m: sum(summaryTrxNsByuKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsByuKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsByuKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsByuKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsByuKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsByuKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsByuKabupaten)
                .where(and(
                    eq(summaryTrxNsByuKabupaten.tgl, currDate),
                    eq(summaryTrxNsByuKabupaten.regional, 'PUMA'),
                ))
                .groupBy(summaryTrxNsByuKabupaten.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_byu).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    targetAll: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevRegional.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.trx_all_m})/(SUM(${regionalTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.trx_all_m})/(${today}/${daysInMonth}*((SUM(${regionalTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevRegional.trx_all_m}) - (SUM(${regionalTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(regionalTerritory)
                .leftJoin(summaryRevRegional, eq(regionalTerritory.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.regional, regionalTargetRevenue.regional))
                .groupBy(regionalTerritory.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const branchTerritory = db
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
                    branch: sql<string>`
                    CASE
                        WHEN kabupaten IN (
                            'AMBON',
                            'KOTA AMBON',
                            'MALUKU TENGAH',
                            'SERAM BAGIAN TIMUR',
                            'KEPULAUAN ARU',
                            'KOTA TUAL',
                            'MALUKU BARAT DAYA',
                            'MALUKU TENGGARA',
                            'MALUKU TENGGARA BARAT',
                            'BURU',
                            'BURU SELATAN',
                            'SERAM BAGIAN BARAT',
                            'KEPULAUAN TANIMBAR'
                        ) THEN 'AMBON'
                        WHEN kabupaten IN (
                            'KOTA JAYAPURA',
                            'JAYAPURA',
                            'KEEROM',
                            'MAMBERAMO RAYA',
                            'SARMI',
                            'BIAK',
                            'BIAK NUMFOR',
                            'KEPULAUAN YAPEN',
                            'SUPIORI',
                            'WAROPEN',
                            'JAYAWIJAYA',
                            'LANNY JAYA',
                            'MAMBERAMO TENGAH',
                            'NDUGA',
                            'PEGUNUNGAN BINTANG',
                            'TOLIKARA',
                            'YAHUKIMO',
                            'YALIMO'
                        ) THEN 'JAYAPURA'
                        WHEN kabupaten IN (
                            'MANOKWARI',
                            'FAKFAK',
                            'FAK FAK',
                            'KAIMANA',
                            'MANOKWARI SELATAN',
                            'PEGUNUNGAN ARFAK',
                            'TELUK BINTUNI',
                            'TELUK WONDAMA',
                            'KOTA SORONG',
                            'MAYBRAT',
                            'RAJA AMPAT',
                            'SORONG',
                            'SORONG SELATAN',
                            'TAMBRAUW'
                        ) THEN 'SORONG'
                        WHEN kabupaten IN (
                            'ASMAT',
                            'BOVEN DIGOEL',
                            'MAPPI',
                            'MERAUKE',
                            'INTAN JAYA',
                            'MIMIKA',
                            'PUNCAK',
                            'PUNCAK JAYA',
                            'TIMIKA',
                            'DEIYAI',
                            'DOGIYAI',
                            'NABIRE',
                            'PANIAI'
                        ) THEN 'TIMIKA'
                        ELSE NULL
                    END
                    `.as('branch'),
                    trx_all_m: sum(summaryTrxNsByuKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsByuKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsByuKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsByuKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsByuKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsByuKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsByuKabupaten)
                .where(and(
                    eq(summaryTrxNsByuKabupaten.tgl, currDate),
                    eq(summaryTrxNsByuKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_byu).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchTerritory.branch,
                    targetAll: sql<number>`ROUND(SUM(${branchTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`SUM(${summaryRevBranch.trx_all_m})`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.trx_all_m})/(SUM(${branchTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.trx_all_m})/(${today}/${daysInMonth}*((SUM(${branchTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevBranch.trx_all_m}) - (SUM(${branchTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(branchTerritory)
                .leftJoin(summaryRevBranch, eq(branchTerritory.branch, sql`b.branch`))
                .leftJoin(branchTargetRevenue, eq(branchTerritory.branch, branchTargetRevenue.branch))
                .groupBy(branchTerritory.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const subbranchTerritory = db
                .select()
                .from(territoryArea4)
                .where(branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryTrxNsByuKabupaten.subbranch,
                    trx_all_m: sum(summaryTrxNsByuKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsByuKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsByuKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsByuKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsByuKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsByuKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsByuKabupaten)
                .where(and(
                    eq(summaryTrxNsByuKabupaten.tgl, currDate),
                    eq(summaryTrxNsByuKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_byu).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchTerritory.subbranch,
                    targetAll: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.trx_all_m})/(SUM(${subbranchTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.trx_all_m})/(${today}/${daysInMonth}*((SUM(${subbranchTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.trx_all_m}) - (SUM(${subbranchTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(subbranchTerritory)
                .leftJoin(summaryRevSubbranch, eq(subbranchTerritory.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchTerritory.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchTerritory.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const clusterTerritory = db
                .select()
                .from(territoryArea4)
                .where(branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster)
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevCluster = db
                .select({
                    cluster: summaryTrxNsByuKabupaten.cluster,
                    trx_all_m: sum(summaryTrxNsByuKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsByuKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsByuKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsByuKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsByuKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsByuKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsByuKabupaten)
                .where(and(
                    eq(summaryTrxNsByuKabupaten.tgl, currDate),
                    eq(summaryTrxNsByuKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_byu).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterTerritory.cluster,
                    targetAll: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCluster.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.trx_all_m})/(SUM(${clusterTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.trx_all_m})/(${today}/${daysInMonth}*((SUM(${clusterTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevCluster.trx_all_m}) - (SUM(${clusterTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(clusterTerritory)
                .leftJoin(summaryRevCluster, eq(clusterTerritory.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterTerritory.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterTerritory.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const kabupatenTerritory = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                })
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
                    kabupaten: summaryTrxNsByuKabupaten.kabupaten,
                    trx_all_m: sum(summaryTrxNsByuKabupaten.trx_all_m).as('trx_all_m'),
                    trx_all_m1: sum(summaryTrxNsByuKabupaten.trx_all_m1).as('trx_all_m1'),
                    trx_all_m12: sum(summaryTrxNsByuKabupaten.trx_all_m12).as('trx_all_m12'),
                    trx_all_y: sum(summaryTrxNsByuKabupaten.trx_all_y).as('trx_all_y'),
                    trx_all_y1: sum(summaryTrxNsByuKabupaten.trx_all_y1).as('trx_all_y1'),
                    trx_all_absolut: sum(summaryTrxNsByuKabupaten.trx_all_absolut).as('trx_all_absolut'),
                })
                .from(summaryTrxNsByuKabupaten)
                .where(and(
                    eq(summaryTrxNsByuKabupaten.tgl, currDate),
                    eq(summaryTrxNsByuKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_trx_ns: sum(feiTargetPuma.trx_ns_byu).as('target_trx_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueKabupaten = db
                .select({
                    name: kabupatenTerritory.kabupaten,
                    targetAll: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10,2)`.as('target_trx_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.trx_all_m}))`.as('trx_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.trx_all_m})/(SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.trx_all_m})/(${today}/${daysInMonth}*((SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.trx_all_m}) - (SUM(${kabupatenTargetRevenue.target_trx_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m1)) / SUM(trx_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(trx_all_m) - SUM(trx_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(trx_all_m) - SUM(trx_all_m12)) / SUM(trx_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(trx_all_y) - SUM(trx_all_y1)) / SUM(trx_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(kabupatenTerritory)
                .leftJoin(summaryRevKabupaten, eq(kabupatenTerritory.kabupaten, summaryRevKabupaten.kabupaten))
                .leftJoin(kabupatenTargetRevenue, eq(kabupatenTerritory.kabupaten, kabupatenTargetRevenue.kabupaten))
                .groupBy(kabupatenTerritory.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json(finalDataRevenue)
        })
    .get('/revenue-new-sales-v2', zValidator('query', z.object({ date: z.coerce.date().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalSubquery = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select()
                .from(summaryRevAllByLosRegional, {
                    useIndex: [
                        index('tgl').on(summaryRevAllByLosRegional.tgl),
                        index('regional').on(summaryRevAllByLosRegional.regional)
                    ]
                })
                .where(and(
                    eq(summaryRevAllByLosRegional.tgl, currDate),
                    eq(summaryRevAllByLosRegional.regional, 'PUMA'),
                ))
                .groupBy(summaryRevAllByLosRegional.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_rev_ns: sum(feiTargetPuma.rev_ns).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalSubquery.regional,
                    targetAll: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_rev_ns}),2)`.as('target_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevRegional.rev_new_sales_m}),2)`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.rev_new_sales_m})/SUM(${regionalTargetRevenue.target_rev_ns}))*100,2),'%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.rev_new_sales_m})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.target_rev_ns}))))*100,2),'%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevRegional.rev_new_sales_m}) - SUM(${regionalTargetRevenue.target_rev_ns}),0)),2)`.as('gap_to_target_ns'),
                    momAll: sql<string>`CONCAT(${summaryRevRegional.rev_new_sales_mom}, '%')`.as('mom_ns'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevRegional.rev_new_sales_absolut}),2)`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevRegional.rev_new_sales_yoy}, '%')`.as('yoy_ns'),
                    qoqAll: sql<string>`CONCAT(${summaryRevRegional.rev_new_sales_qoq}, '%')`.as('qoq_ns'),
                    ytdAll: sql<string>`CONCAT(${summaryRevRegional.rev_new_sales_ytd}, '%')`.as('ytd_ns'),
                })
                .from(regionalSubquery)
                .leftJoin(summaryRevRegional, eq(regionalSubquery.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalSubquery.regional, regionalTargetRevenue.regional))
                .groupBy(regionalSubquery.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    qoqAll: sql<string>`''`.as('qoq_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
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
                .select()
                .from(summaryRevAllByLosBranch, {
                    useIndex: [
                        index('tgl').on(summaryRevAllByLosBranch.tgl),
                        index('regional').on(summaryRevAllByLosBranch.regional),
                        index('branch').on(summaryRevAllByLosBranch.branch),
                    ]
                })
                .where(and(
                    eq(summaryRevAllByLosBranch.tgl, currDate),
                    eq(summaryRevAllByLosBranch.regional, 'PUMA'),
                ))
                .groupBy(summaryRevAllByLosBranch.branch)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_rev_ns: sum(feiTargetPuma.rev_ns).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchSubquery.branch,
                    targetAll: sql<number>`ROUND(SUM(${branchTargetRevenue.target_rev_ns}),2)`.as('target_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevBranch.rev_new_sales_m}),2)`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.rev_new_sales_m})/SUM(${branchTargetRevenue.target_rev_ns}))*100,2),'%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.rev_new_sales_m})/(${today}/${daysInMonth}*(SUM(${branchTargetRevenue.target_rev_ns}))))*100,2),'%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevBranch.rev_new_sales_m}) - SUM(${branchTargetRevenue.target_rev_ns}),0)),2)`.as('gap_to_target_ns'),
                    momAll: sql<string>`CONCAT(${summaryRevBranch.rev_new_sales_mom}, '%')`.as('mom_ns'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevBranch.rev_new_sales_absolut}),2)`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevBranch.rev_new_sales_yoy}, '%')`.as('yoy_ns'),
                    qoqAll: sql<string>`CONCAT(${summaryRevBranch.rev_new_sales_qoq}, '%')`.as('qoq_ns'),
                    ytdAll: sql<string>`CONCAT(${summaryRevBranch.rev_new_sales_ytd}, '%')`.as('ytd_ns'),
                })
                .from(branchSubquery)
                .leftJoin(summaryRevBranch, eq(branchSubquery.branch, summaryRevBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchSubquery.branch, branchTargetRevenue.branch))
                .groupBy(branchSubquery.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    qoqAll: sql<string>`''`.as('qoq_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
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
                .select()
                .from(summaryRevAllByLosSubbranch, {
                    useIndex: [
                        index('tgl').on(summaryRevAllByLosSubbranch.tgl),
                        index('regional').on(summaryRevAllByLosSubbranch.regional),
                        index('branch').on(summaryRevAllByLosSubbranch.branch),
                        index('subbranch').on(summaryRevAllByLosSubbranch.subbranch),
                    ]
                })
                .where(and(
                    eq(summaryRevAllByLosSubbranch.tgl, currDate),
                    eq(summaryRevAllByLosSubbranch.regional, 'PUMA'),
                ))
                .groupBy(summaryRevAllByLosSubbranch.subbranch)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_rev_ns: sum(feiTargetPuma.rev_ns).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchSubquery.subbranch,
                    targetAll: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_rev_ns}),2)`.as('target_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.rev_new_sales_m}),2)`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.rev_new_sales_m})/SUM(${subbranchTargetRevenue.target_rev_ns}))*100,2),'%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.rev_new_sales_m})/(${today}/${daysInMonth}*(SUM(${subbranchTargetRevenue.target_rev_ns}))))*100,2),'%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevSubbranch.rev_new_sales_m}) - SUM(${subbranchTargetRevenue.target_rev_ns}),0)),2)`.as('gap_to_target_ns'),
                    momAll: sql<string>`CONCAT(${summaryRevSubbranch.rev_new_sales_mom}, '%')`.as('mom_ns'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevSubbranch.rev_new_sales_absolut}),2)`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevSubbranch.rev_new_sales_yoy}, '%')`.as('yoy_ns'),
                    qoqAll: sql<string>`CONCAT(${summaryRevSubbranch.rev_new_sales_qoq}, '%')`.as('qoq_ns'),
                    ytdAll: sql<string>`CONCAT(${summaryRevSubbranch.rev_new_sales_ytd}, '%')`.as('ytd_ns'),
                })
                .from(subbranchSubquery)
                .leftJoin(summaryRevSubbranch, eq(subbranchSubquery.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchSubquery.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchSubquery.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    qoqAll: sql<string>`''`.as('qoq_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
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
                .select()
                .from(summaryRevAllByLosCluster, {
                    useIndex: [
                        index('tgl').on(summaryRevAllByLosCluster.tgl),
                        index('regional').on(summaryRevAllByLosCluster.regional),
                        index('branch').on(summaryRevAllByLosCluster.branch),
                        index('subbranch').on(summaryRevAllByLosCluster.subbranch),
                        index('cluster').on(summaryRevAllByLosCluster.cluster),
                    ]
                })
                .where(and(
                    eq(summaryRevAllByLosCluster.tgl, currDate),
                    eq(summaryRevAllByLosCluster.regional, 'PUMA'),
                ))
                .groupBy(summaryRevAllByLosCluster.cluster)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_rev_ns: sum(feiTargetPuma.rev_ns).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterSubquery.cluster,
                    targetAll: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_rev_ns}),2)`.as('target_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCluster.rev_new_sales_m}),2)`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.rev_new_sales_m})/SUM(${clusterTargetRevenue.target_rev_ns}))*100,2),'%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.rev_new_sales_m})/(${today}/${daysInMonth}*(SUM(${clusterTargetRevenue.target_rev_ns}))))*100,2),'%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevCluster.rev_new_sales_m}) - SUM(${clusterTargetRevenue.target_rev_ns}),0)),2)`.as('gap_to_target_ns'),
                    momAll: sql<string>`CONCAT(${summaryRevCluster.rev_new_sales_mom}, '%')`.as('mom_ns'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevCluster.rev_new_sales_absolut}),2)`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevCluster.rev_new_sales_yoy}, '%')`.as('yoy_ns'),
                    qoqAll: sql<string>`CONCAT(${summaryRevCluster.rev_new_sales_qoq}, '%')`.as('qoq_ns'),
                    ytdAll: sql<string>`CONCAT(${summaryRevCluster.rev_new_sales_ytd}, '%')`.as('ytd_ns'),
                })
                .from(clusterSubquery)
                .leftJoin(summaryRevCluster, eq(clusterSubquery.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterSubquery.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterSubquery.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    qoqAll: sql<string>`''`.as('qoq_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
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
                .select()
                .from(summaryRevAllByLosKabupaten, {
                    useIndex: [
                        index('tgl').on(summaryRevAllByLosKabupaten.tgl),
                        index('regional').on(summaryRevAllByLosKabupaten.regional),
                        index('branch').on(summaryRevAllByLosKabupaten.branch),
                        index('subbranch').on(summaryRevAllByLosKabupaten.subbranch),
                        index('cluster').on(summaryRevAllByLosKabupaten.cluster),
                        index('kabupaten').on(summaryRevAllByLosKabupaten.kabupaten),
                    ]
                })
                .where(and(
                    eq(summaryRevAllByLosKabupaten.tgl, currDate),
                    eq(summaryRevAllByLosKabupaten.regional, 'PUMA'),
                ))
                .groupBy(summaryRevAllByLosKabupaten.kabupaten)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_rev_ns: sum(feiTargetPuma.rev_ns).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueKabupaten = db
                .select({
                    name: kabupatenSubquery.kabupaten,
                    targetAll: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.target_rev_ns}),2)`.as('target_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.rev_new_sales_m}),2)`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.rev_new_sales_m})/SUM(${kabupatenTargetRevenue.target_rev_ns}))*100,2),'%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.rev_new_sales_m})/(${today}/${daysInMonth}*(SUM(${kabupatenTargetRevenue.target_rev_ns}))))*100,2),'%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND((COALESCE(SUM(${summaryRevKabupaten.rev_new_sales_m}) - SUM(${kabupatenTargetRevenue.target_rev_ns}),0)),2)`.as('gap_to_target_ns'),
                    momAll: sql<string>`CONCAT(${summaryRevKabupaten.rev_new_sales_mom}, '%')`.as('mom_ns'),
                    revAllAbsolut: sql<number>`ROUND(SUM(${summaryRevKabupaten.rev_new_sales_absolut}),2)`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`CONCAT(${summaryRevKabupaten.rev_new_sales_yoy}, '%')`.as('yoy_ns'),
                    qoqAll: sql<string>`CONCAT(${summaryRevKabupaten.rev_new_sales_qoq}, '%')`.as('qoq_ns'),
                    ytdAll: sql<string>`CONCAT(${summaryRevKabupaten.rev_new_sales_ytd}, '%')`.as('ytd_ns'),
                })
                .from(kabupatenSubquery)
                .leftJoin(summaryRevKabupaten, eq(kabupatenSubquery.kabupaten, summaryRevKabupaten.kabupaten))
                .leftJoin(kabupatenTargetRevenue, eq(kabupatenSubquery.kabupaten, kabupatenTargetRevenue.kabupaten))
                .groupBy(kabupatenSubquery.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json(finalDataRevenue, 200)
        })
    .get('/revenue-new-sales-prabayar-v2', zValidator('query', z.object({ date: z.coerce.date().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalTerritory = db
                .select({ regional: territoryArea4.regional })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const summaryRevRegional = db
                .select({
                    regional: summaryRevNsPrabayarKabupaten.regional,
                    rev_all_m: sum(summaryRevNsPrabayarKabupaten.rev_all_m).as('rev_all_m'),
                    rev_all_m1: sum(summaryRevNsPrabayarKabupaten.rev_all_m1).as('rev_all_m1'),
                    rev_all_m12: sum(summaryRevNsPrabayarKabupaten.rev_all_m12).as('rev_all_m12'),
                    rev_all_y: sum(summaryRevNsPrabayarKabupaten.rev_all_y).as('rev_all_y'),
                    rev_all_y1: sum(summaryRevNsPrabayarKabupaten.rev_all_y1).as('rev_all_y1'),
                    rev_all_absolut: sum(summaryRevNsPrabayarKabupaten.rev_all_absolut).as('rev_all_absolut'),
                })
                .from(summaryRevNsPrabayarKabupaten)
                .where(and(
                    eq(summaryRevNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryRevNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(summaryRevNsPrabayarKabupaten.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_rev_ns: sum(feiTargetPuma.rev_ns_prepaid).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    targetAll: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_rev_ns}) * 10,2)`.as('target_rev_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevRegional.rev_all_m}))`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.rev_all_m})/(SUM(${regionalTargetRevenue.target_rev_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevRegional.rev_all_m})/(${today}/${daysInMonth}*((SUM(${regionalTargetRevenue.target_rev_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevRegional.rev_all_m}) - (SUM(${regionalTargetRevenue.target_rev_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m1)) / SUM(rev_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(rev_all_m) - SUM(rev_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m12)) / SUM(rev_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(rev_all_y) - SUM(rev_all_y1)) / SUM(rev_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(regionalTerritory)
                .leftJoin(summaryRevRegional, eq(regionalTerritory.regional, summaryRevRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.regional, regionalTargetRevenue.regional))
                .groupBy(regionalTerritory.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const branchTerritory = db
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
                    branch: summaryRevNsPrabayarKabupaten.branch,
                    rev_all_m: sum(summaryRevNsPrabayarKabupaten.rev_all_m).as('rev_all_m'),
                    rev_all_m1: sum(summaryRevNsPrabayarKabupaten.rev_all_m1).as('rev_all_m1'),
                    rev_all_m12: sum(summaryRevNsPrabayarKabupaten.rev_all_m12).as('rev_all_m12'),
                    rev_all_y: sum(summaryRevNsPrabayarKabupaten.rev_all_y).as('rev_all_y'),
                    rev_all_y1: sum(summaryRevNsPrabayarKabupaten.rev_all_y1).as('rev_all_y1'),
                    rev_all_absolut: sum(summaryRevNsPrabayarKabupaten.rev_all_absolut).as('rev_all_absolut'),
                })
                .from(summaryRevNsPrabayarKabupaten)
                .where(and(
                    eq(summaryRevNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryRevNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_rev_ns: sum(feiTargetPuma.rev_ns_prepaid).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const revenueBranch = db
                .select({
                    name: branchTerritory.branch,
                    targetAll: sql<number>`ROUND(SUM(${branchTargetRevenue.target_rev_ns}) * 10,2)`.as('target_rev_ns'),
                    revAll: sql<number>`SUM(${summaryRevBranch.rev_all_m})`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.rev_all_m})/(SUM(${branchTargetRevenue.target_rev_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevBranch.rev_all_m})/(${today}/${daysInMonth}*((SUM(${branchTargetRevenue.target_rev_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevBranch.rev_all_m}) - (SUM(${branchTargetRevenue.target_rev_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m1)) / SUM(rev_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(rev_all_m) - SUM(rev_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m12)) / SUM(rev_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(rev_all_y) - SUM(rev_all_y1)) / SUM(rev_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(branchTerritory)
                .leftJoin(summaryRevBranch, eq(branchTerritory.branch, sql`b.branch`))
                .leftJoin(branchTargetRevenue, eq(branchTerritory.branch, branchTargetRevenue.branch))
                .groupBy(branchTerritory.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const subbranchTerritory = db
                .select()
                .from(territoryArea4)
                .where(branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryRevNsPrabayarKabupaten.subbranch,
                    rev_all_m: sum(summaryRevNsPrabayarKabupaten.rev_all_m).as('rev_all_m'),
                    rev_all_m1: sum(summaryRevNsPrabayarKabupaten.rev_all_m1).as('rev_all_m1'),
                    rev_all_m12: sum(summaryRevNsPrabayarKabupaten.rev_all_m12).as('rev_all_m12'),
                    rev_all_y: sum(summaryRevNsPrabayarKabupaten.rev_all_y).as('rev_all_y'),
                    rev_all_y1: sum(summaryRevNsPrabayarKabupaten.rev_all_y1).as('rev_all_y1'),
                    rev_all_absolut: sum(summaryRevNsPrabayarKabupaten.rev_all_absolut).as('rev_all_absolut'),
                })
                .from(summaryRevNsPrabayarKabupaten)
                .where(and(
                    eq(summaryRevNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryRevNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_rev_ns: sum(feiTargetPuma.rev_ns_prepaid).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const revenueSubbranch = db
                .select({
                    name: subbranchTerritory.subbranch,
                    targetAll: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_rev_ns}) * 10,2)`.as('target_rev_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.rev_all_m}))`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.rev_all_m})/(SUM(${subbranchTargetRevenue.target_rev_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevSubbranch.rev_all_m})/(${today}/${daysInMonth}*((SUM(${subbranchTargetRevenue.target_rev_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevSubbranch.rev_all_m}) - (SUM(${subbranchTargetRevenue.target_rev_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m1)) / SUM(rev_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(rev_all_m) - SUM(rev_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m12)) / SUM(rev_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(rev_all_y) - SUM(rev_all_y1)) / SUM(rev_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(subbranchTerritory)
                .leftJoin(summaryRevSubbranch, eq(subbranchTerritory.subbranch, summaryRevSubbranch.subbranch))
                .leftJoin(subbranchTargetRevenue, eq(subbranchTerritory.subbranch, subbranchTargetRevenue.subbranch))
                .groupBy(subbranchTerritory.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const clusterTerritory = db
                .select()
                .from(territoryArea4)
                .where(branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster)
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const summaryRevCluster = db
                .select({
                    cluster: summaryRevNsPrabayarKabupaten.cluster,
                    rev_all_m: sum(summaryRevNsPrabayarKabupaten.rev_all_m).as('rev_all_m'),
                    rev_all_m1: sum(summaryRevNsPrabayarKabupaten.rev_all_m1).as('rev_all_m1'),
                    rev_all_m12: sum(summaryRevNsPrabayarKabupaten.rev_all_m12).as('rev_all_m12'),
                    rev_all_y: sum(summaryRevNsPrabayarKabupaten.rev_all_y).as('rev_all_y'),
                    rev_all_y1: sum(summaryRevNsPrabayarKabupaten.rev_all_y1).as('rev_all_y1'),
                    rev_all_absolut: sum(summaryRevNsPrabayarKabupaten.rev_all_absolut).as('rev_all_absolut'),
                })
                .from(summaryRevNsPrabayarKabupaten)
                .where(and(
                    eq(summaryRevNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryRevNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_rev_ns: sum(feiTargetPuma.rev_ns_prepaid).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const revenueCluster = db
                .select({
                    name: clusterTerritory.cluster,
                    targetAll: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_rev_ns}) * 10,2)`.as('target_rev_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevCluster.rev_all_m}))`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.rev_all_m})/(SUM(${clusterTargetRevenue.target_rev_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevCluster.rev_all_m})/(${today}/${daysInMonth}*((SUM(${clusterTargetRevenue.target_rev_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevCluster.rev_all_m}) - (SUM(${clusterTargetRevenue.target_rev_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m1)) / SUM(rev_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(rev_all_m) - SUM(rev_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m12)) / SUM(rev_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(rev_all_y) - SUM(rev_all_y1)) / SUM(rev_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(clusterTerritory)
                .leftJoin(summaryRevCluster, eq(clusterTerritory.cluster, summaryRevCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterTerritory.cluster, clusterTargetRevenue.cluster))
                .groupBy(clusterTerritory.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    targetAll: sql<number>`''`.as('target_ns'),
                    revAll: sql<number>`''`.as('rev_ns'),
                    achTargetFmAll: sql<string>`''`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`''`.as('drr_ns'),
                    gapToTargetAll: sql<number>`''`.as('gap_to_target_ns'),
                    momAll: sql<string>`''`.as('mom_ns'),
                    revAllAbsolut: sql<number>`''`.as('rev_ns_absolut'),
                    yoyAll: sql<string>`''`.as('yoy_ns'),
                    ytdAll: sql<string>`''`.as('ytd_ns'),
                })
                .from(feiTargetPuma)

            const kabupatenTerritory = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                })
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
                    kabupaten: summaryRevNsPrabayarKabupaten.kabupaten,
                    rev_all_m: sum(summaryRevNsPrabayarKabupaten.rev_all_m).as('rev_all_m'),
                    rev_all_m1: sum(summaryRevNsPrabayarKabupaten.rev_all_m1).as('rev_all_m1'),
                    rev_all_m12: sum(summaryRevNsPrabayarKabupaten.rev_all_m12).as('rev_all_m12'),
                    rev_all_y: sum(summaryRevNsPrabayarKabupaten.rev_all_y).as('rev_all_y'),
                    rev_all_y1: sum(summaryRevNsPrabayarKabupaten.rev_all_y1).as('rev_all_y1'),
                    rev_all_absolut: sum(summaryRevNsPrabayarKabupaten.rev_all_absolut).as('rev_all_absolut'),
                })
                .from(summaryRevNsPrabayarKabupaten)
                .where(and(
                    eq(summaryRevNsPrabayarKabupaten.tgl, currDate),
                    eq(summaryRevNsPrabayarKabupaten.regional, 'PUMA'),
                ))
                .groupBy(sql`1`)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_rev_ns: sum(feiTargetPuma.rev_ns_prepaid).as('target_rev_ns')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const revenueKabupaten = db
                .select({
                    name: kabupatenTerritory.kabupaten,
                    targetAll: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.target_rev_ns}) * 10,2)`.as('target_rev_ns'),
                    revAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.rev_all_m}))`.as('rev_ns'),
                    achTargetFmAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.rev_all_m})/(SUM(${kabupatenTargetRevenue.target_rev_ns}) * 10))*100, 2), '%')`.as('ach_target_fm_ns'),
                    drrAll: sql<string>`CONCAT(ROUND((SUM(${summaryRevKabupaten.rev_all_m})/(${today}/${daysInMonth}*((SUM(${kabupatenTargetRevenue.target_rev_ns}) * 10))))*100, 2), '%')`.as('drr_ns'),
                    gapToTargetAll: sql<number>`ROUND(SUM(${summaryRevKabupaten.rev_all_m}) - (SUM(${kabupatenTargetRevenue.target_rev_ns}) * 10), 2)`.as('gap_to_target'),
                    momAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m1)) / SUM(rev_all_m1) * 100, 2),'%')`.as('mom'),
                    revAllAbsolut: sql<number>`ROUND(SUM(rev_all_m) - SUM(rev_all_m1), 2)`.as('absolut'),
                    yoyAll: sql<string>`CONCAT(ROUND((SUM(rev_all_m) - SUM(rev_all_m12)) / SUM(rev_all_m12) * 100, 2),'%')`.as('yoy'),
                    ytdAll: sql<string>`CONCAT(ROUND((SUM(rev_all_y) - SUM(rev_all_y1)) / SUM(rev_all_y1) * 100, 2),'%')`.as('ytd')
                })
                .from(kabupatenTerritory)
                .leftJoin(summaryRevKabupaten, eq(kabupatenTerritory.kabupaten, summaryRevKabupaten.kabupaten))
                .leftJoin(kabupatenTargetRevenue, eq(kabupatenTerritory.kabupaten, kabupatenTargetRevenue.kabupaten))
                .groupBy(kabupatenTerritory.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json(finalDataRevenue, 200)
        })

export default app