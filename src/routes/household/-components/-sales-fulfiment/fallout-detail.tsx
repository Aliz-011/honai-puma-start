import React, { useEffect, useRef } from 'react';
import * as d3 from 'd3';
import { endOfMonth, format, subMonths } from "date-fns"

import { type SalesFulfilmentResponseData } from '@/types';

export const FalloutDetail = ({ data, selectedDate }: { data: SalesFulfilmentResponseData[]; selectedDate: Date }) => {
    const svgRef = useRef<SVGSVGElement>(null);

    useEffect(() => {
        const svg = d3.select(svgRef.current);
        svg.selectAll("*").remove();

        const width = 1100;
        const height = 800;
        const margin = { top: 20, right: 200, bottom: 20, left: 20 };

        svg.attr("width", width).attr("height", height);

        const nodesData = [
            // Level 0
            { id: 0, name: "RE NON PS", value: data[0].total_re_non_ps, level: 0, percentage: '0%', color: "#374151", x: 0, y: 0, height: 200 },

            // Level 1
            { id: 1, name: "FALLOUT", value: data[0].fallout, level: 1, percentage: data[0].fallout_per, color: "#7F1D1D", x: 0, y: 0, height: 200 },
            { id: 2, name: "PROVISION_ISSUED", value: data[0].provision_issued, level: 1, percentage: data[0].provision_issued_per, color: "#374151", x: 0, y: 0, height: 200 },
            { id: 3, name: "OTHERS", value: data[0].provision_completed + data[0].registration + data[0].activation_completed, level: 1, percentage: (parseFloat(data[0].provision_completed_per.replace('%', '')) + parseFloat(data[0].registration_per.replace('%', '')) + parseFloat(data[0].activation_completed_per.replace('%', ''))).toFixed(2) + '%', color: "#374151", x: 0, y: 0, height: 200 },

            // Level 2
            { id: 4, name: "KENDALA TEKNIK", value: data[0].kendala_teknik, level: 2, percentage: (data[0].kendala_teknik / data[0].fallout * 100).toFixed(2) + '%', color: "#C2410C", x: 0, y: 0, height: 200 },
            { id: 5, name: "KENDALA PELANGGAN", value: data[0].kendala_pelanggan, level: 2, percentage: (data[0].kendala_pelanggan / data[0].fallout * 100).toFixed(2) + '%', color: "#DC2626", x: 0, y: 0, height: 200 },
            { id: 6, name: "KENDALA SISTEM", value: data[0].kendala_sistem, level: 2, percentage: (data[0].kendala_sistem / data[0].fallout * 100).toFixed(2) + '%', color: "#C2410C", x: 0, y: 0, height: 200 },
            { id: 7, name: "OTHERS", value: data[0].kendala_others, level: 2, percentage: (data[0].kendala_others / data[0].fallout * 100).toFixed(2) + '%', color: "#C2410C", x: 0, y: 0, height: 200 },
        ];

        const linksData = [
            // Level 0 to Level 1
            { source: 0, target: 1, value: data[0].fallout },
            { source: 0, target: 2, value: data[0].provision_issued },
            { source: 0, target: 3, value: data[0].provision_completed + data[0].registration + data[0].activation_completed },

            // Level 1 to Level 2
            { source: 1, target: 4, value: data[0].kendala_teknik },
            { source: 1, target: 5, value: data[0].kendala_pelanggan },
            { source: 1, target: 6, value: data[0].kendala_sistem },
            { source: 1, target: 7, value: data[0].kendala_others },
        ];

        const sankeyWidth = width - margin.left - margin.right;
        const sankeyHeight = height - margin.top - margin.bottom;

        // Position nodes by level
        const levelWidth = sankeyWidth / 2;
        const nodesByLevel = d3.group(nodesData, d => d.level);

        nodesData.forEach(node => {
            node.x = margin.left + node.level * levelWidth;

            const levelNodes = nodesByLevel.get(node.level)!;
            const levelHeight = sankeyHeight / levelNodes.length;
            const nodeIndex = levelNodes.findIndex(n => n.id === node.id);
            node.y = margin.top + margin.bottom + nodeIndex * levelHeight + levelHeight / 2;

            // Set consistent node height for all nodes
            node.height = 60;
        });

        const g = svg.append("g");

        // Create links
        const link = g.selectAll(".link")
            .data(linksData)
            .enter().append("path")
            .attr("class", "link")
            .attr("d", d => {
                const source = nodesData[d.source];
                const target = nodesData[d.target];
                const linkWidth = Math.max(1, (d.value / 2113) * 200);

                return `M${source.x + 120},${source.y}
                C${source.x + 120 + (target.x - source.x) / 2},${source.y}
                 ${target.x - (target.x - source.x) / 2},${target.y}
                 ${target.x - 20},${target.y}`;
            })
            .style("stroke", "#94A3B8")
            .style("stroke-opacity", 0.6)
            .style("stroke-width", 1.5)
            .style("fill", "none");

        // Create nodes
        const node = g.selectAll(".node")
            .data(nodesData)
            .enter().append("g")
            .attr("class", "node")
            .attr("transform", d => `translate(${d.x},${d.y - d.height / 2})`);

        node.append("rect")
            .attr("width", 140)
            .attr("height", d => d.height)
            .style("fill", d => d.color)
            .style("stroke", "#000")
            .style("stroke-width", 1)
            .style("opacity", 0.9);

        // Add node labels
        node.append("text")
            .attr("x", 70)
            .attr("y", d => d.level > 0 ? 15 : 20)
            .attr("dy", "0.35em")
            .attr("text-anchor", "middle")
            .style("fill", "white")
            .style("font-size", "12px")
            .style("font-weight", "bold")
            .text(d => d.name);

        // Add value labels
        node.append("text")
            .attr("x", 70)
            .attr("y", d => d.level > 0 ? 30 : 35)
            .attr("dy", "0.35em")
            .attr("text-anchor", "middle")
            .style("fill", "white")
            .style("font-size", "14px")
            .style("font-weight", "bold")
            .text(d => d.value);

        // Add percentage labels for non-root nodes
        node.filter(d => d.level > 0)
            .append("text")
            .attr("x", 70)
            .attr("y", d => 45)
            .attr("dy", "0.35em")
            .attr("text-anchor", "middle")
            .style("fill", "white")
            .style("font-size", "14px")
            .style("font-weight", "bold")
            .text(d => d.percentage);

        // Add hover effects
        node.filter(d => d.level > 0).on("mouseover", function (event, d) {
            d3.select(this).select("rect").style("opacity", 1);

            // Show tooltip
            const tooltip = d3.select("body").append("div")
                .attr("class", "tooltip")
                .style("position", "absolute")
                .style("background", "rgba(0,0,0,0.8)")
                .style("color", "white")
                .style("padding", "8px")
                .style("border-radius", "4px")
                .style("font-size", "12px")
                .style("pointer-events", "none")
                .style("opacity", 0);

            tooltip.transition().duration(200).style("opacity", 1);
            tooltip.html(`<strong>${d.name}</strong><br/>Actual: ${d.value}<br/>Cont: ${d.percentage}`)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 10) + "px");
        })
            .on("mouseout", function (event, d) {
                d3.select(this).select("rect").style("opacity", 0.9);
                d3.selectAll(".tooltip").remove();
            });
    }, [])

    const technicalIssues = [
        { name: "ODP JAUH", value: data[0].ODP_JAUH, percentage: (data[0].ODP_JAUH / data[0].kendala_teknik * 100).toFixed(2), color: "#EAB308" },
        { name: "ODP FULL", value: data[0].ODP_FULL, percentage: (data[0].ODP_FULL / data[0].kendala_teknik * 100).toFixed(2), color: "#EAB308" },
        { name: "TIANG", value: data[0].TIANG, percentage: (data[0].TIANG / data[0].kendala_teknik * 100).toFixed(2), color: "#DC2626" },
        { name: "KENDALA JALUR/RUTE", value: data[0].JALUR_RUTE, percentage: (data[0].JALUR_RUTE / data[0].kendala_teknik * 100).toFixed(2), color: "#DC2626" },
        { name: "CROSS JALAN", value: data[0].CROSS_JALAN, percentage: (data[0].CROSS_JALAN / data[0].kendala_teknik * 100).toFixed(2), color: "#DC2626" },
        { name: "TIDAK ADA ODP", value: data[0].TIDAK_ADA_ODP, percentage: (data[0].TIDAK_ADA_ODP / data[0].kendala_teknik * 100).toFixed(2), color: "#DC2626" },
        { name: "ODP RETI", value: data[0].ODP_RETI, percentage: (data[0].ODP_RETI / data[0].kendala_teknik * 100).toFixed(2), color: "#DC2626" },
        { name: "KENDALA IKR/IKG", value: data[0].KENDALA_IKR_IKG, percentage: (data[0].KENDALA_IKR_IKG / data[0].kendala_teknik * 100).toFixed(2), color: "#DC2626" },
        { name: "ODP RUSAK", value: data[0].ODP_RUSAK, percentage: (data[0].ODP_RUSAK / data[0].kendala_teknik * 100).toFixed(2), color: "#DC2626" },
        { name: "ODP BELUM GO LIVE", value: data[0].ODP_BELUM_GO_LIVE, percentage: (data[0].ODP_BELUM_GO_LIVE / data[0].kendala_teknik * 100).toFixed(2), color: "#DC2626" },
    ]

    const customerIssues = [
        { name: "PENDING", value: data[0].PENDING, percentage: (data[0].PENDING / data[0].kendala_pelanggan * 100).toFixed(2), color: "#DC2626" },
        { name: "RNA", value: data[0].RNA, percentage: (data[0].RNA / data[0].kendala_pelanggan * 100).toFixed(2), color: "#DC2626" },
        { name: "BATAL", value: data[0].BATAL, percentage: (data[0].BATAL / data[0].kendala_pelanggan * 100).toFixed(2), color: "#DC2626" },
        { name: "RUMAH KOSONG", value: data[0].RUMAH_KOSONG, percentage: (data[0].RUMAH_KOSONG / data[0].kendala_pelanggan * 100).toFixed(2), color: "#DC2626" },
        { name: "GANTI PAKET", value: data[0].GANTI_PAKET, percentage: (data[0].GANTI_PAKET / data[0].kendala_pelanggan * 100).toFixed(2), color: "#DC2626" },
        { name: "DOUBLE INPUT", value: data[0].DOUBLE_INPUT, percentage: (data[0].DOUBLE_INPUT / data[0].kendala_pelanggan * 100).toFixed(2), color: "#DC2626" },
        { name: "ALAMAT TIDAK DITEMUKAN", value: data[0].ALAMAT_TIDAK_DITEMUKAN, percentage: (data[0].ALAMAT_TIDAK_DITEMUKAN / data[0].kendala_pelanggan * 100).toFixed(2), color: "#DC2626" }
    ]

    const lastDayOfSelectedMonth = endOfMonth(selectedDate);
    const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

    const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
    const endOfPrevMonth = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1);

    const currDate = format(endOfCurrMonth, 'dd MMM yyyy')
    const prevMonth = format(endOfPrevMonth, 'MMM')

    return (
        <div className="w-full overflow-x-auto bg-white dark:bg-white/[0.03] p-4">
            <div className="mb-4">
                <h2 className="text-2xl font-bold text-gray-800">FUNNELING REG. PUMA</h2>
                <p className="text-gray-600 italic">2 Month (Last update date: {currDate})</p>
            </div>
            <div className="flex gap-4">
                {/* Sankey Diagram */}
                <svg ref={svgRef}></svg>

                <div className="flex flex-col gap-6">
                    {/* Technical Issues Table */}
                    <div className="bg-white dark:bg-white/[0.03] rounded-lg shadow-md overflow-hidden w-full">
                        <div className="bg-orange-700 text-white p-3">
                            <h3 className="text-lg font-bold text-center">KENDALA TEKNIK ({data[0].kendala_teknik} cases)</h3>
                        </div>
                        <div className="overflow-x-auto w-full">
                            <table className="w-full text-sm">
                                <thead className="bg-orange-100">
                                    <tr>
                                        <th className="px-3 py-2 text-left font-semibold">Symptom</th>
                                        <th className="px-3 py-2 text-center font-semibold">Total</th>
                                        <th className="px-3 py-2 text-center font-semibold">%</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {technicalIssues.map((item, index) => (
                                        <tr key={index} className={index % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                                            <td className="px-3 py-2 font-medium">{item.name}</td>
                                            <td className="px-3 py-2 text-center font-bold">{item.value}</td>
                                            <td className="px-3 py-2 text-center">
                                                <span className={`px-2 py-1 rounded text-white text-xs ${item.value >= 20 ? 'bg-red-500' :
                                                    item.value >= 5 ? 'bg-orange-500' :
                                                        item.value >= 1 ? 'bg-yellow-500' : 'bg-green-500'
                                                    }`}>
                                                    {item.percentage}%
                                                </span>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>

                    {/* Customer Issues Table */}
                    <div className="bg-white dark:bg-white/[0.03] rounded-lg shadow-md overflow-hidden w-full">
                        <div className="bg-red-700 text-white p-3">
                            <h3 className="text-lg font-bold text-center">KENDALA PELANGGAN ({data[0].kendala_pelanggan} cases)</h3>
                        </div>
                        <div className="overflow-x-auto w-full">
                            <table className="w-full text-sm">
                                <thead className="bg-red-100">
                                    <tr>
                                        <th className="px-3 py-2 text-left font-semibold">Symptom</th>
                                        <th className="px-3 py-2 text-center font-semibold">Total</th>
                                        <th className="px-3 py-2 text-center font-semibold">%</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {customerIssues.map((item, index) => (
                                        <tr key={index} className={index % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                                            <td className="px-3 py-2 font-medium">{item.name}</td>
                                            <td className="px-3 py-2 text-center font-bold">{item.value}</td>
                                            <td className="px-3 py-2 text-center">
                                                <span className={`px-2 py-1 rounded text-white text-xs ${item.value >= 5 ? 'bg-red-500' :
                                                    item.value >= 2 ? 'bg-orange-500' :
                                                        item.value >= 1 ? 'bg-yellow-500' : 'bg-green-500'
                                                    }`}>
                                                    {item.percentage}%
                                                </span>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}