import { BarChart3, TrendingDown, TrendingUp } from "lucide-react";

import { cn } from "@/lib/utils";
import { type DemandsDeploymentResponseData } from "@/types"

export const DataTable = ({ data }: { data: DemandsDeploymentResponseData[]; }) => {

    const alproMom = (((data[0].used / data[0].amount_port) - (data[0].used_m1 / data[0].amount_port_m1)) / (data[0].used_m1 / data[0].amount_port_m1) * 100)

    return (
        <div className="bg-white/80 backdrop-blur-xl rounded-2xl shadow-xl border border-white/20 p-6 hover:shadow-2xl transition-all duration-300">
            <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-bold text-gray-800 flex items-center">
                    <div className="w-1 h-8 bg-gradient-to-b from-orange-400 to-orange-600 rounded-full mr-3"></div>
                    Alpro Profiling
                </h2>
                <BarChart3 className="w-5 h-5 text-orange-500" />
            </div>

            <div className="rounded-xl">
                <div className="bg-gradient-to-r from-gray-50 to-gray-100/50 rounded-xl py-2.5 px-4 hover:shadow-md transition-all duration-200">
                    <div className="grid grid-cols-7 gap-4 items-center text-sm w-full">
                        <div className="font-semibold text-gray-700">Territory</div>
                        <div className="font-semibold text-gray-700">Amount Port</div>
                        <div className="font-semibold text-gray-700">Avail. Port</div>
                        <div className="font-semibold text-gray-700">Occ. Alpro</div>
                        <div className="font-semibold text-gray-700">Occ. Alpro M1</div>
                        <div className="font-semibold text-gray-700">Alpro MoM</div>
                        <div className="font-semibold text-gray-700">ODP</div>
                    </div>
                </div>

                <div className="grid grid-cols-7 gap-4 text-sm px-4">
                    <div className="font-medium text-gray-900 py-2">{data[0].name}</div>
                    <div className="font-sans tabular-nums text-gray-900 py-2">{data[0].amount_port.toLocaleString('id-ID')}</div>
                    <div className="font-sans tabular-nums text-gray-900 py-2">{data[0].avai_port.toLocaleString('id-ID')}</div>
                    <div className="font-sans tabular-nums text-gray-900 py-2">{(data[0].used / data[0].amount_port * 100).toFixed(2).replace('.', ',')}</div>
                    <div className="font-sans tabular-nums text-gray-900 py-2">{(data[0].used_m1 / data[0].amount_port_m1 * 100).toFixed(2).replace('.', ',')}</div>
                    <div className="flex items-center py-2">
                        {alproMom > 0 ? <TrendingUp className={cn("w-4 h-4 mr-1", alproMom > 0 ? 'text-green-600' : 'text-red-600')} /> : <TrendingDown className={cn("w-4 h-4 mr-1", alproMom > 0 ? 'text-green-600' : 'text-red-600')} />}
                        <span className={cn("font-sans tabular-nums font-semibold", alproMom > 0 ? 'text-green-600' : 'text-red-600')}>{alproMom.toFixed(2).replace('.', ',')}%</span>
                    </div>
                    <div className="font-sans tabular-nums text-gray-900 py-2">{Number(data[0].total_odp).toLocaleString('id-ID')}</div>
                </div>
            </div>
        </div>
    )
}

export const DataTableODP = ({ data }: { data: DemandsDeploymentResponseData[]; }) => {
    const formattedData = [
        {
            status: 'RED',
            color: 'bg-red-500',
            amount_port: data[0].amount_port_red,
            avai_port: data[0].avai_port_red,
            occ_alpro: (data[0].used_red / data[0].amount_port_red * 100).toFixed(2).replace('.', ','),
            occ_alpro_m1: (data[0].used_red_m1 / data[0].amount_port_red_m1 * 100).toFixed(2).replace('.', ','),
            occ_alpro_mom: (((data[0].used_red / data[0].amount_port_red) - (data[0].used_red_m1 / data[0].amount_port_red_m1)) / (data[0].used_red_m1 / data[0].amount_port_red_m1) * 100),
            total_odp: data[0].total_odp_red,
            odp_per: (data[0].total_odp_red / data[0].total_odp * 100).toFixed(2) + '%'
        },
        {
            status: 'BLACK',
            color: 'bg-gray-800',
            amount_port: data[0].amount_port_black,
            avai_port: data[0].avai_port_black,
            occ_alpro: (data[0].used_black / data[0].amount_port_black * 100).toFixed(2).replace('.', ','),
            occ_alpro_m1: (data[0].used_black_m1 / data[0].amount_port_black_m1 * 100).toFixed(2).replace('.', ','),
            occ_alpro_mom: (((data[0].used_black / data[0].amount_port_black) - (data[0].used_black_m1 / data[0].amount_port_black_m1)) / (data[0].used_black_m1 / data[0].amount_port_black_m1) * 100),
            total_odp: data[0].total_odp_black,
            odp_per: (data[0].total_odp_black / data[0].total_odp * 100).toFixed(2) + '%'
        },
        {
            status: 'GREEN',
            color: 'bg-green-500',
            amount_port: data[0].amount_port_green,
            avai_port: data[0].avai_port_green,
            occ_alpro: (data[0].used_green / data[0].amount_port_green * 100).toFixed(2).replace('.', ','),
            occ_alpro_m1: (data[0].used_green_m1 / data[0].amount_port_green_m1 * 100).toFixed(2).replace('.', ','),
            occ_alpro_mom: (((data[0].used_green / data[0].amount_port_green) - (data[0].used_green_m1 / data[0].amount_port_green_m1)) / (data[0].used_green_m1 / data[0].amount_port_green_m1) * 100),
            total_odp: data[0].total_odp_green,
            odp_per: (data[0].total_odp_green / data[0].total_odp * 100).toFixed(2) + '%'
        },
        {
            status: 'YELLOW',
            color: 'bg-yellow-500',
            amount_port: data[0].amount_port_yellow,
            avai_port: data[0].avai_port_yellow,
            occ_alpro: (data[0].used_yellow / data[0].amount_port_yellow * 100).toFixed(2).replace('.', ','),
            occ_alpro_m1: (data[0].used_yellow_m1 / data[0].amount_port_yellow_m1 * 100).toFixed(2).replace('.', ','),
            occ_alpro_mom: (((data[0].used_yellow / data[0].amount_port_yellow) - (data[0].used_yellow_m1 / data[0].amount_port_yellow_m1)) / (data[0].used_yellow_m1 / data[0].amount_port_yellow_m1) * 100),
            total_odp: data[0].total_odp_yellow,
            odp_per: (data[0].total_odp_yellow / data[0].total_odp * 100).toFixed(2) + '%'
        },

    ]

    return (
        <div className="bg-white/80 backdrop-blur-xl rounded-2xl shadow border border-white/20 p-6 hover:shadow-2xl transition-all duration-300">
            <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-bold text-gray-800 flex items-center">
                    <div className="w-1 h-8 bg-gradient-to-b from-orange-400 to-orange-600 rounded-full mr-3"></div>
                    OCC ODP Status
                </h2>
                <div className="flex space-x-2">
                    <div className="w-3 h-3 bg-red-500 rounded-full"></div>
                    <div className="w-3 h-3 bg-black rounded-full"></div>
                    <div className="w-3 h-3 bg-green-500 rounded-full"></div>
                    <div className="w-3 h-3 bg-yellow-500 rounded-full"></div>
                </div>
            </div>

            <div className="space-y-3">
                <div className="bg-gradient-to-r from-gray-50 to-gray-100/50 rounded-xl p-4 hover:shadow-md transition-all duration-200">
                    <div className="grid grid-cols-8 gap-4 items-center text-sm">
                        <div className="font-semibold text-gray-700">Status</div>
                        <div className="font-semibold text-gray-700">Amount Port</div>
                        <div className="font-semibold text-gray-700">Avai. Port</div>
                        <div className="font-semibold text-gray-700">Occ. Alpro</div>
                        <div className="font-semibold text-gray-700">Occ. Alpro M1</div>
                        <div className="font-semibold text-gray-700">Alpro MoM</div>
                        <div className="font-semibold text-gray-700">ODP</div>
                        <div className="font-semibold text-gray-700">ODP %</div>
                    </div>
                </div>
                {formattedData.map((item, index) => (
                    <div key={index} className="bg-gradient-to-r from-gray-50 to-gray-100/50 rounded-xl p-4 hover:shadow-md transition-all duration-200">
                        <div className="grid grid-cols-8 gap-4 items-center text-sm">
                            <div className="flex items-center">
                                <div className={`w-4 h-4 ${item.color} rounded-full mr-3 shadow-sm`}></div>
                                <span className="font-semibold text-gray-700">{item.status}</span>
                            </div>
                            <div className="font-sans tabular-nums text-gray-900">{item.amount_port.toLocaleString('id-ID')}</div>
                            <div className="font-sans tabular-nums text-gray-900">{item.avai_port.toLocaleString('id-ID')}</div>
                            <div className="font-sans tabular-nums text-gray-900">{item.occ_alpro}</div>
                            <div className="font-sans tabular-nums text-gray-900">{item.occ_alpro_m1}</div>
                            <div className={cn("font-sans tabular-nums font-semibold", item.occ_alpro_mom > 0 ? "text-green-600" : 'text-red-600')}>{item.occ_alpro_mom.toFixed(2)}%</div>
                            <div className="font-sans tabular-nums text-gray-900">{Number(item.total_odp).toLocaleString('id-ID')}</div>
                            <div className="font-sans tabular-nums text-gray-900 font-semibold">{item.odp_per}</div>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    )
}