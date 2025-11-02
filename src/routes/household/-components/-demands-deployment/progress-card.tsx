import { Search } from "lucide-react";

import { cn } from "@/lib/utils";

type ProgressCardParams = {
    data: {
        label: string;
        total_port: number;
        used_port: number;
        ach: string;
        className?: string;
    }[];
    title: string;
}

export const ProgressCard = ({ data, title }: ProgressCardParams) => {
    return (
        <div className="bg-white dark:bg-white/3 rounded-lg shadow-md overflow-hidden w-full h-fit">
            <div className="bg-red-700 text-white p-3">
                <h3 className="text-lg font-bold">{title}</h3>
            </div>
            <div className="overflow-x-auto w-full">
                <table className="w-full text-sm">
                    <thead className="bg-red-100">
                        <tr>
                            <th className="px-3 py-2 text-left font-semibold">Golive</th>
                            <th className="px-3 py-2 text-start font-semibold">Total</th>
                            <th className="px-3 py-2 text-start font-semibold">Used</th>
                            <th className="px-3 py-2 text-start font-semibold">%Occ</th>
                        </tr>
                    </thead>
                    <tbody>
                        {data.map((item, index) => (
                            <tr key={index} className={index % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                                <td className="px-3 py-2 font-normal">{item.label}</td>
                                <td className="px-3 py-2 text-start font-bold">{Number(item.total_port).toLocaleString('id-ID')}</td>
                                <td className="px-3 py-2 text-start font-bold">{Number(item.used_port).toLocaleString('id-ID')}</td>
                                <td className="px-3 py-2 text-start">
                                    <span className={`px-2 py-1 font-normal`}>
                                        {item.ach}
                                    </span>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export const GoliveCard = ({ data, title }: {
    data: {
        label: string;
        value: number | string;
        className?: string;
    }[];
    title: string;
}) => {
    return (
        <div className="bg-white/95 backdrop-blur-lg rounded-lg p-4 border relative overflow-hidden">
            <div className="flex items-center gap-3 mb-4">
                <h3 className="text-lg font-semibold text-gray-800">{title}</h3>
            </div>

            <div className="space-y-2">
                {data.map((item, index) => (
                    <div key={index} className="flex justify-between px-2 items-center bg-linear-to-r from-gray-50 to-gray-100/50 rounded-xl hover:shadow-md transition-all duration-200">
                        <span className="text-gray-600 font-medium text-base">{item.label}</span>
                        <span className={cn("px-2 py-1.5 rounded-lg font-bold text-base", item.className)}>
                            {item.value}
                        </span>
                    </div>
                ))}
            </div>
        </div>
    )
}

export const DemandData = ({ data }: { data: { metric: string, value: number | string }[] }) => {
    return (
        <div className="bg-white/80 backdrop-blur-xl rounded-2xl shadow-xl border border-white/20 p-6 hover:shadow-2xl transition-all duration-300">
            <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-bold text-gray-800 flex items-center">
                    <div className="w-1 h-8 bg-linear-to-b from-orange-400 to-orange-600 rounded-full mr-3"></div>
                    Demand Analysis
                </h2>
                <Search className="w-5 h-5 text-orange-500" />
            </div>

            <div className="overflow-x-auto">
                <table className="w-full">
                    <tbody>
                        {data.map((row, index) => (
                            <tr key={index} className="border-b border-gray-100 hover:bg-gray-50 transition-colors last:border-none">
                                <td className="p-3 text-gray-900 font-medium">{row.metric}</td>
                                <td className="p-3 text-gray-900 font-semibold">{row.value}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    )
}