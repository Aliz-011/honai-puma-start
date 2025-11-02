import {
    Table,
    TableBody,
    TableCell,
    TableFooter,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table"

export const DataTable = () => {
    return (
        <Table>
            <TableHeader className="bg-muted sticky top-0 z-10">
                <TableRow className="bg-accent hover:bg-accent">
                    <TableHead rowSpan={2} className="w-[100px] text-center rounded-tl-md">Regional</TableHead>
                    <TableHead rowSpan={2} className="text-center">Branch</TableHead>
                    <TableHead rowSpan={2} className="text-center">Cluster</TableHead>
                    <TableHead rowSpan={2} className="text-center">Champion</TableHead>
                    <TableHead rowSpan={2} className="text-center bg-orange-300">Trx</TableHead>
                    <TableHead rowSpan={2} className="text-center bg-orange-300 rounded-tr-md">Subs</TableHead>
                </TableRow>
            </TableHeader>
            <TableBody className="**:data-[slot=table-cell]:first:w-8">
                <TableRow className="border-0 border-none">
                    <TableCell>MALUKU DAN PAPUA</TableCell>
                    <TableCell className="text-center text-sm">JAYAPURA</TableCell>
                    <TableCell className="text-center text-sm">KOTA JAYAPURA</TableCell>
                    <TableCell className="text-center text-sm">Y</TableCell>
                    <TableCell className="text-center text-sm">192</TableCell>
                    <TableCell className="text-center text-sm">200</TableCell>
                </TableRow>
            </TableBody>
            <TableFooter>
                <TableRow>
                    <TableCell colSpan={4} className="bg-accent text-center rounded-bl-md">MALUKU DAN PAPUA</TableCell>
                    <TableCell className="text-center bg-orange-300">2201</TableCell>
                    <TableCell className="text-center bg-orange-300 rounded-br-md">2302</TableCell>
                </TableRow>
            </TableFooter>
        </Table>
    )
}