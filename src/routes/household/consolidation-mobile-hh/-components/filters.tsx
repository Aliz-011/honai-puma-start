import { useQuery } from '@tanstack/react-query'
import DatePicker from 'react-datepicker'
import { format, subDays } from 'date-fns'

import "react-datepicker/dist/react-datepicker.css";

import { Button } from "@/components/ui/button"
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select"
import {
    Field,
    FieldLabel
} from "@/components/ui/field"
import { getBranches, getClusters, getKabupatens } from "@/data/territories"

import { parseAsString, useQueryState } from 'nuqs';
import { useMemo } from 'react';
import { Skeleton } from '@/components/ui/skeleton';

export const Filters = ({ daysBehind, isLoading }: { daysBehind: number, isLoading?: boolean }) => {
    const [date, setDate] = useQueryState('date', parseAsString.withDefault(format(subDays(new Date(), daysBehind), 'yyyy-MM-dd')))
    const [branch, setBranch] = useQueryState('branch', parseAsString.withDefault(''))
    const [cluster, setCluster] = useQueryState('cluster', parseAsString.withDefault(''))
    const [kabupaten, setKabupaten] = useQueryState('kabupaten', parseAsString.withDefault(''))

    const { data: branches = [] } = useQuery({
        queryKey: ['branches'],
        queryFn: async () => await getBranches(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
        refetchOnWindowFocus: false
    })

    const { data: clusters = [] } = useQuery({
        queryKey: ['clusters'],
        queryFn: async () => await getClusters(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
        refetchOnWindowFocus: false
    })

    const { data: kabupatens = [] } = useQuery({
        queryKey: ['kabupatens'],
        queryFn: async () => await getKabupatens(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
        refetchOnWindowFocus: false
    })


    const filteredClusters = useMemo(() => {
        if (!branch) {
            const validSubbranchIds = new Set(branches.map(b => b.territory))
            return clusters.filter(cluster => validSubbranchIds.has(cluster.branch))
        }
        return clusters.filter(cluster => cluster.branch === branch)
    }, [branches, clusters, branch])

    const filteredKabupatens = useMemo(() => {
        if (!cluster) {
            const validClusterIds = new Set(clusters.map(b => b.territory))
            return kabupatens.filter(kabupaten => validClusterIds.has(kabupaten.cluster))
        }
        return kabupatens.filter(kabupaten => kabupaten.cluster === cluster)
    }, [clusters, kabupatens, cluster])


    const handleBranchChange = (value: string) => {
        setBranch(value);
        setCluster("");
        setKabupaten("");
    };

    const handleClusterChange = (value: string) => {
        setCluster(value);
        setKabupaten("");
    };

    const handleDateChange = (date: Date | null) => {
        const notNullDate = date ? date : subDays(new Date(), daysBehind)
        setDate(format(notNullDate, 'yyyy-MM-dd'))
    }

    if (isLoading) {
        return (
            <div className='grid grid-cols-3 sm:grid-cols- md:grid-cols-4 lg:grid-cols-4 gap-4'>
                {[1, 2, 3, 4].map((_, index) => (
                    <div className='space-y-2' key={index}>
                        <Skeleton className='h-4 w-10' />
                        <Skeleton className='h-8 w-48' />
                    </div>
                ))}
            </div>
        )
    }

    const renderMonthContent = (
        month: number,
        shortMonth: string,
        longMonth: string,
        day: Date
    ) => {
        const fullYear = new Date(day).getFullYear();
        const tooltipText = `Tooltip for month: ${longMonth} ${fullYear}`;

        return <span title={tooltipText}>{shortMonth}</span>;
    };

    return (
        <div className='grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 lg:grid-cols-4 gap-4'>
            <div className='w-full max-w-md'>
                <Field>
                    <FieldLabel>Tanggal</FieldLabel>
                    <DatePicker
                        selected={date ? new Date(date) : subDays(new Date(), daysBehind)}
                        renderMonthContent={renderMonthContent}
                        onChange={(date) => handleDateChange(date)}
                        dateFormat="yyyy-MM"
                        maxDate={subDays(new Date(), daysBehind)}
                        minDate={new Date(2025, 0, 1)}
                        className="w-full text-gray-700 bg-white border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                        calendarClassName="shadow-lg border-0"
                        customInput={
                            <input className="w-full h-8 px-2 py-1.5 text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-900 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent cursor-pointer" />
                        }
                        wrapperClassName="w-full"
                        showPopperArrow={false}
                        showMonthYearPicker
                    />
                </Field>
            </div>
            <div className="w-full max-w-md">
                <Field>
                    <FieldLabel>Region</FieldLabel>
                    <Select value='MALUKU DAN PAPUA'>
                        <SelectTrigger>
                            <SelectValue placeholder="Region" />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value='MALUKU DAN PAPUA'>MALUKU DAN PAPUA</SelectItem>
                        </SelectContent>
                    </Select>
                </Field>
            </div>
            <div className="w-full max-w-md">
                <Field>
                    <FieldLabel>Branch</FieldLabel>
                    <Select value={branch} onValueChange={handleBranchChange}>
                        <SelectTrigger>
                            <SelectValue placeholder="Branch" />
                        </SelectTrigger>
                        <SelectContent>
                            {branches.map((branch) => (
                                <SelectItem key={branch.territory!} value={branch.territory!}>
                                    {branch.territory}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </Field>
            </div>
            <div className="w-full max-w-md">
                <Field>
                    <FieldLabel>Cluster</FieldLabel>
                    <Select value={cluster} onValueChange={handleClusterChange} disabled={!branch}>
                        <SelectTrigger>
                            <SelectValue placeholder="Cluster" />
                        </SelectTrigger>
                        <SelectContent>
                            {filteredClusters.map((cluster) => (
                                <SelectItem key={cluster.territory!} value={cluster.territory!}>
                                    {cluster.territory}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </Field>
            </div>
            <div className="w-full max-w-md">
                <Field>
                    <FieldLabel>Kabupaten</FieldLabel>
                    <Select value={kabupaten} onValueChange={setKabupaten} disabled={!cluster}>
                        <SelectTrigger>
                            <SelectValue placeholder="Kabupaten" />
                        </SelectTrigger>
                        <SelectContent>
                            {filteredKabupatens.map((kabupaten) => (
                                <SelectItem key={kabupaten.territory!} value={kabupaten.territory!}>
                                    {kabupaten.territory}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </Field>
            </div>
            <div className="space-y-2 mt-auto">
                <Button onClick={() => {
                    setBranch('')
                    setCluster('')
                    setDate(format(subDays(new Date(), 2), 'yyyy-MM-dd'))
                }} disabled={!branch || (!branch && !cluster)} className="cursor-pointer">
                    Clear Filter
                </Button>
            </div>
        </div>
    )
}