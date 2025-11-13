import { useQuery } from '@tanstack/react-query'
import DatePicker from 'react-datepicker'
import { Funnel } from 'lucide-react';
import { format, subDays } from 'date-fns'
import { parseAsString, useQueryState } from 'nuqs'

import "react-datepicker/dist/react-datepicker.css";

import { Label } from '@/components/ui/label'
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select"

import { Skeleton } from '@/components/ui/skeleton';
import { getBranches, getWoks } from '@/data/territories';
import { useMemo } from 'react';

export const Filter = ({ daysBehind, disabled = false }: { daysBehind: number, disabled?: boolean }) => {
    const [date, setDate] = useQueryState('date', parseAsString.withDefault(''))
    const [branch, setBranch] = useQueryState('branch', parseAsString.withDefault(''))
    const [wok, setWok] = useQueryState('wok', parseAsString.withDefault(''))

    const { data: branches = [], isLoading } = useQuery({
        queryKey: ['branch'],
        queryFn: async () => await getBranches(),
        staleTime: 60 * 1000 * 60 * 24,
        gcTime: 60 * 1000 * 15,
        retry: 2,
        refetchOnWindowFocus: false
    })

    const { data: woks = [], isLoading: isLoadingWok } = useQuery({
        queryKey: ['woks'],
        queryFn: async () => await getWoks(),
        staleTime: 60 * 1000 * 60 * 24,
        gcTime: 60 * 1000 * 15,
        retry: 2,
        refetchOnWindowFocus: false
    })

    const getFilteredWoks = useMemo(() => {
        if (!branch) {
            const validBranchIds = new Set(branches.map(b => b.territory))
            return woks.filter(wok => validBranchIds.has(wok.branch))
        }
        return woks.filter(wok => wok.branch === branch)
    }, [branches, woks, branch])

    const handleBranchChange = (value: string) => {
        setBranch(value);
        setWok("");
    };

    const handleWokChange = (value: string) => {
        setWok(value);
    };

    const handleDateChange = (date: Date | null) => {
        const safeDate = date ?? new Date()
        setDate(format(safeDate, 'yyyy-MM-dd'));
    }

    if (isLoading || !branches) {
        return (
            <div className='grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 lg:grid-cols-4 gap-4'>
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
        _: number,
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
            <div className='space-y-2'>
                <Label>Tanggal</Label>
                <DatePicker
                    selected={date ? new Date(date) : subDays(new Date(), daysBehind)}
                    renderMonthContent={renderMonthContent}
                    onChange={(date) => handleDateChange(date)}
                    dateFormat="yyyy-MM-dd"
                    maxDate={subDays(new Date(), daysBehind)}
                    minDate={new Date(2025, 0, 1)}
                    className="w-full text-gray-700 bg-white border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    calendarClassName="shadow-lg border-0"
                    customInput={
                        <input className="w-full h-8 px-2 py-1.5 text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-900 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent cursor-pointer" />
                    }
                    wrapperClassName="w-full"
                    showPopperArrow={false}
                    showDateSelect
                />
            </div>
            <div className='space-y-2'>
                <Label>Branch</Label>
                <Select onValueChange={handleBranchChange} defaultValue="" value={branch}>
                    <SelectTrigger className='w-full'>
                        <SelectValue placeholder='Select Branch' />
                    </SelectTrigger>
                    <SelectContent>
                        {branches.map(branch => (
                            <SelectItem key={branch.territory} value={branch.territory!}>{branch.territory}</SelectItem>
                        ))}
                    </SelectContent>
                </Select>
            </div>
            <div className='space-y-2'>
                <Label>WOK</Label>
                <Select onValueChange={handleWokChange} defaultValue="" value={wok}>
                    <SelectTrigger disabled={!branch || isLoadingWok} className='w-full'>
                        <SelectValue placeholder='Select WOK' />
                    </SelectTrigger>
                    <SelectContent>
                        {getFilteredWoks.map(wok => (
                            <SelectItem key={wok.wok} value={wok.wok!}>{wok.wok}</SelectItem>
                        ))}
                    </SelectContent>
                </Select>
            </div>
            <div className="space-y-2 mt-auto">
                <button onClick={() => {
                    setBranch('')
                    setWok('')
                }} disabled={!branch || disabled} className="cursor-pointer px-4 py-2 bg-linear-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 text-white text-xs font-medium rounded-xl transition-all duration-200 shadow-lg hover:shadow-xl disabled:pointer-events-none disabled:opacity-50">
                    <Funnel className="w-3 h-3 inline mr-1" />
                    Clear Filter
                </button>
            </div>
        </div>
    )
}