import React, { Fragment } from "react"
import type { QueryObserverResult, RefetchOptions } from "@tanstack/react-query"

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
import { cn } from "@/lib/utils"

type Params = {
    data?: Revenue[];
    isLoading?: boolean;
    title: string;
}

export const DataTable = ({ title, data, isLoading }: Params) => {
    const isPayload = title.includes('PAYLOAD')
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

    return (
        <Card>
            <CardHeader className="flex items-center justify-between">
                <CardTitle>{title}</CardTitle>
            </CardHeader>
            <CardContent>
                <Table>
                    <TableCaption>A list of territories and their revenues</TableCaption>
                    <TableHeader>
                        <TableRow>
                            <TableHead rowSpan={2} className="font-medium border-r dark:border-gray-700 text-white text-center bg-black">Territory</TableHead>
                            <TableHead colSpan={6} className="font-medium border bg-red-700 text-gray-50 text-center dark:text-white dark:border-gray-800">{isPayload ? 'Payload' : 'Rev'} Mobile (In Bio)</TableHead>
                            <TableHead colSpan={6} className="font-medium border bg-red-600 text-gray-50 text-center dark:text-white dark:border-gray-800">{isPayload ? 'Payload' : 'Rev'} HH (In Bio)</TableHead>
                            <TableHead colSpan={6} className="font-medium border bg-red-700 text-gray-50 text-center dark:text-white dark:border-gray-800">{isPayload ? 'Payload' : 'Rev'} Consolidation (In Bio)</TableHead>
                        </TableRow>
                        <TableRow>
                            {[...Array(3)].map((_, index) => (
                                <Fragment key={index}>
                                    <TableHead className="font-medium bg-red-500 text-gray-50 text-center dark:text-white dark:border-gray-800">Mtd</TableHead>
                                    <TableHead className="font-medium bg-red-500 text-gray-50 text-center dark:text-white dark:border-gray-800">M1</TableHead>
                                    <TableHead className="font-medium bg-red-500 text-gray-50 text-center dark:text-white dark:border-gray-800">%MoM</TableHead>
                                    <TableHead className="font-medium bg-red-500 text-gray-50 text-center dark:text-white dark:border-gray-800">Ytd</TableHead>
                                    <TableHead className="font-medium bg-red-500 text-gray-50 text-center dark:text-white dark:border-gray-800">Y-1</TableHead>
                                    <TableHead className="font-medium bg-red-500 text-gray-50 text-center dark:text-white dark:border-gray-800">%Ytd</TableHead>
                                </Fragment>
                            ))}
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {data?.map((item, index) => {
                            const isHeaderRow = ['BRANCH', 'SUBBRANCH', 'CLUSTER', 'KABUPATEN'].includes(item.territory?.toUpperCase()!);

                            return (
                                <TableRow key={`${item.territory}-${index}`}>
                                    <TableCell
                                        colSpan={isHeaderRow ? 19 : 1}
                                        className={cn(
                                            "px-1 py-0.5 border-r last:border-r-0 text-start",
                                            isHeaderRow ? 'font-semibold bg-gray-200 dark:text-white dark:border-gray-800 dark:bg-white/3' : 'font-normal dark:text-white dark:border-gray-800'
                                        )}
                                    >
                                        {item.territory}
                                    </TableCell>

                                    {!isHeaderRow && (
                                        <>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.rev_all_m : formatToBillion(item.rev_all_m)}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.rev_all_m1 : formatToBillion(item.rev_all_m1)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!", getValue(item.rev_all_mom))}>
                                                <span className='text-end'>{item.rev_all_mom}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.rev_all_y : formatToBillion(item.rev_all_y)}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.rev_all_y1 : formatToBillion(item.rev_all_y1)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!", getValue(item.rev_all_ytd))}>
                                                <span className='text-end'>{item.rev_all_ytd}</span>
                                            </TableCell>

                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.rev_fix_m : formatToBillion(item.rev_fix_m)}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.rev_fix_m1 : formatToBillion(item.rev_fix_m1)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!", getValue(item.rev_fix_mom))}>
                                                <span className='text-end'>{item.rev_fix_mom}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.rev_fix_y : formatToBillion(item.rev_fix_y)}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.rev_fix_y1 : formatToBillion(item.rev_fix_y1)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!", getValue(item.rev_fix_ytd))}>
                                                <span className='text-end'>{item.rev_fix_ytd}</span>
                                            </TableCell>

                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.cons_m : formatToBillion(item.cons_m)}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.cons_m1 : formatToBillion(item.cons_m1)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!", getValue(item.cons_mom))}>
                                                <span className='text-end'>{item.cons_mom}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.cons_y : formatToBillion(item.cons_y)}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!">
                                                <span className='text-end'>{isPayload ? item.cons_y1 : formatToBillion(item.cons_y1)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums!", getValue(item.cons_ytd))}>
                                                <span className='text-end'>{item.cons_ytd}</span>
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

function formatToBillion(value: string | null) {
    return (parseInt(value ?? "0") / 1000000000).toLocaleString('id-ID', {
        maximumFractionDigits: 2,
        minimumFractionDigits: 0,
    })
}

function getValue(value: string | null) {
    const val = value ? Number(value.replace('%', '')) : 0

    if (val < 0) {
        return "bg-red-300 text-red-700"
    }

    return "bg-green-300 text-green-700"
}

export type Revenue = {
    territory: string | null;
    rev_all_m: string | null;
    rev_all_m1: string | null;
    rev_all_mom: string | null;
    rev_all_y: string | null;
    rev_all_y1: string | null;
    rev_all_ytd: string | null;
    rev_fix_m: string | null;
    rev_fix_m1: string | null;
    rev_fix_mom: string | null;
    rev_fix_y: string | null;
    rev_fix_y1: string | null;
    rev_fix_ytd: string | null;
    cons_m: string | null;
    cons_m1: string | null;
    cons_mom: string | null;
    cons_y: string | null;
    cons_y1: string | null;
    cons_ytd: string | null;
}