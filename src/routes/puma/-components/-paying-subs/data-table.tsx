import React from "react"
import { ArrowUpDown } from "lucide-react"
import type { QueryObserverResult, RefetchOptions } from "@tanstack/react-query"
import { endOfMonth, intlFormat, subDays } from "date-fns"

import { Button } from "@/components/ui/button"
import {
    Table,
    TableBody,
    TableCaption,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table"
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'

import { Skeleton } from "@/components/ui/skeleton"
import { cn, formatToBillion, getAchGrowthColor, getGrowthColor } from "@/lib/utils"

type Params = {
    data?: Revenue[];
    date?: string;
    latestUpdatedData: number;
    isLoading?: boolean;
    title: string;
    refetch: (options?: RefetchOptions) => Promise<QueryObserverResult<Revenue[], Error>>
}

export const DataTable = ({ latestUpdatedData: daysBehind, refetch, title, data, isLoading, date }: Params) => {
    const selectedDate = date ? new Date(date) : subDays(new Date(), daysBehind) // today - 2 days
    const lastDayOfSelectedMonth = endOfMonth(selectedDate);
    const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

    const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;

    if (isLoading) {
        return (
            <div className="w-[1104px] overflow-x-auto remove-scrollbar">
                <div className="w-full">
                    <div className="flex flex-col space-y-3">
                        <Skeleton className="h-4 w-[200px]" />
                        <Skeleton className="h-[275px] w-[1104px] rounded-xl" />
                    </div>
                </div>
            </div>
        )
    }

    if (!data) {
        return (
            <div>
                <p>Not Found</p>
                <Button onClick={() => refetch()}>Reload</Button>
            </div>
        )
    }

    return (
        <Card>
            <CardHeader>
                <CardTitle>{title}</CardTitle>
            </CardHeader>
            <CardContent>
                <Table>
                    <TableCaption>A list of territories and their revenues</TableCaption>
                    <TableHeader>
                        <TableRow>
                            <TableHead rowSpan={2} className="font-medium border-r dark:border-gray-700 text-white text-center bg-rose-700">Territory</TableHead>
                            <TableHead colSpan={12} className="font-medium border bg-blue-500 text-gray-50 text-center dark:text-white dark:border-gray-800">{title}</TableHead>
                        </TableRow>
                        <TableRow>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">Target</TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">{intlFormat(endOfCurrMonth, { month: 'short', day: 'numeric' }, { locale: "id-ID" })}</TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                Gap
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    Ach FM
                                    <ArrowUpDown className="ml-2 h-4 w-4" />
                                </div>
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    Ach DRR
                                    <ArrowUpDown className="ml-2 h-4 w-4" />
                                </div>
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    MoM(%)
                                    <ArrowUpDown className="ml-2 h-4 w-4" />
                                </div>
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    YoY
                                    <ArrowUpDown className="ml-2 h-4 w-4" />
                                </div>
                            </TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        <TableRow>
                            <TableCell colSpan={14} className="px-1 py-0.5 border-r last:border-r-0 text-start font-semibold bg-gray-200 dark:text-white dark:border-gray-800 dark:bg-white/[0.03]">
                                REGION
                            </TableCell>
                        </TableRow>

                        {data.map((item, index) => {
                            const isHeaderRow = ['REGIONAL', 'BRANCH', 'SUBBRANCH', 'CLUSTER', 'KABUPATEN'].includes(item.name?.toUpperCase()!);

                            return (
                                <TableRow key={`${item.name}-${index}`}>
                                    <TableCell
                                        colSpan={isHeaderRow ? 14 : 1}
                                        className={cn(
                                            "px-1 py-0.5 border-r last:border-r-0 text-start",
                                            isHeaderRow ? 'font-semibold bg-gray-200 dark:text-white dark:border-gray-800 dark:bg-white/[0.03]' : 'font-normal dark:text-white dark:border-gray-800'
                                        )}
                                    >
                                        {item.name}
                                    </TableCell>

                                    {!isHeaderRow && (
                                        <>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums">
                                                <span className='text-end'>{formatToBillion(item.target_rgb_all)}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums">
                                                <span className='text-end'>{formatToBillion(item.mtd_rgb)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs dark:text-white dark:border-gray-800 tabular-nums", Number(item.gap_to_target_rgb) > 0 ? 'text-green-500' : 'text-rose-500')}>
                                                <span>{formatToBillion(Number(item.gap_to_target_rgb))}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs dark:text-white dark:border-gray-800 tabular-nums", getAchGrowthColor(item.ach_target_fm_rgb) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.ach_target_fm_rgb}
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs dark:text-white dark:border-gray-800 tabular-nums", getAchGrowthColor(item.drr_rgb) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.drr_rgb}
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs font-medium dark:border-gray-800 tabular-nums", getGrowthColor(item.mom_rgb) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.mom_rgb}
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs dark:text-white dark:border-gray-800 tabular-nums", getGrowthColor(item.yoy_rgb) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.yoy_rgb}
                                            </TableCell>
                                        </>
                                    )}
                                </TableRow>
                            );
                        })}

                    </TableBody>
                </Table>
            </CardContent>
        </Card>
    )
}

function formatToPercentage(val: number) {
    return val.toLocaleString('id-ID', {
        style: 'percent',
        maximumFractionDigits: 2
    })
}

interface Revenue {
    name: string | null;
    target_rgb_all: number;
    mtd_rgb: number;
    mom_rgb: string;
    yoy_rgb: string;
    ach_target_fm_rgb: string;
    drr_rgb: string;
    gap_to_target_rgb: string;
    penetration_to_cb: string;
    mtd_rgb_data: number;
    mom_rgb_data: string;
    yoy_rgb_data: string;
    mtd_rgb_digital: number;
    mom_rgb_digital: string;
    yoy_rgb_digital: string;
    mtd_arpu: number;
    mom_arpu: string;
    yoy_arpu: string;
    mtd_arpu_data: number;
    mom_arpu_data: string;
    yoy_arpu_data: string;
    mtd_arpu_digital: number;
    mom_arpu_digital: string;
    yoy_arpu_digital: string;
    mtd_rpmb: number;
    mom_rpmb: string;
    yoy_rpmb: string;
}