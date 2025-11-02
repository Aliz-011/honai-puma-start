'use client'

import { useMutation, useQueryClient } from "@tanstack/react-query"
import {
    LogOutIcon,
    MoreVerticalIcon,
    UserCircleIcon,
} from "lucide-react"
import { signOut } from "@hono/auth-js/react"
import { useNavigate } from "@tanstack/react-router"

import {
    Avatar,
    AvatarFallback,
    AvatarImage,
} from "@/components/ui/avatar"
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuGroup,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
    useSidebar,
} from "@/components/ui/sidebar"

import { useCurrentSession } from "@/hooks/use-current-session"

export function NavUser() {
    const { isMobile } = useSidebar()
    const navigate = useNavigate()
    const { data } = useCurrentSession()
    const queryClient = useQueryClient()

    const mutation = useMutation({
        mutationFn: async () => await signOut()
        ,
        onSuccess: () => {
            navigate({ to: '/login' })
            queryClient.invalidateQueries({ queryKey: ['current-session'] })
        }
    })

    return (
        <SidebarMenu>
            <SidebarMenuItem>
                <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                        <SidebarMenuButton
                            size="lg"
                            className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
                        >
                            <Avatar className="h-8 w-8 rounded-lg grayscale">
                                <AvatarImage src='https://github.com/shadcn.png' alt={data?.user?.name || 'FC'} />
                                <AvatarFallback className="rounded-lg">{data?.user?.name ? data?.user?.name.split(' ')[0] : 'FC'}</AvatarFallback>
                            </Avatar>
                            <div className="grid flex-1 text-left text-sm leading-tight">
                                <span className="truncate font-medium">{data?.user?.username}</span>
                                <span className="truncate text-xs text-muted-foreground">
                                    {data?.user?.email}
                                </span>
                            </div>
                            <MoreVerticalIcon className="ml-auto size-4" />
                        </SidebarMenuButton>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent
                        className="w-[--radix-dropdown-menu-trigger-width] min-w-56 rounded-lg"
                        side={isMobile ? "bottom" : "right"}
                        align="end"
                        sideOffset={4}
                    >
                        <DropdownMenuLabel className="p-0 font-normal">
                            <div className="flex items-center gap-2 px-1 py-1.5 text-left text-sm">
                                <Avatar className="h-8 w-8 rounded-lg">
                                    <AvatarImage src='https://github.com/shadcn.png' alt={data?.user?.name || "FC"} />
                                    <AvatarFallback className="rounded-lg">CN</AvatarFallback>
                                </Avatar>
                                <div className="grid flex-1 text-left text-sm leading-tight">
                                    <span className="truncate font-medium">{data?.user?.name}</span>
                                    <span className="truncate text-xs text-muted-foreground">
                                        {data?.user?.email}
                                    </span>
                                </div>
                            </div>
                        </DropdownMenuLabel>
                        <DropdownMenuSeparator />
                        <DropdownMenuGroup>
                            <DropdownMenuItem className="cursor-pointer" onClick={() => navigate({ to: '/puma/account' })}>
                                <UserCircleIcon />
                                Account
                            </DropdownMenuItem>
                        </DropdownMenuGroup>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem className="cursor-pointer" asChild>
                            <form onSubmit={(e) => {
                                e.preventDefault()
                                e.stopPropagation()

                                mutation.mutate()
                            }}
                                className="w-full"
                            >
                                <button className="inline-flex items-center gap-1 w-full">
                                    <LogOutIcon />
                                    Log out
                                </button>
                            </form>
                        </DropdownMenuItem>
                    </DropdownMenuContent>
                </DropdownMenu>
            </SidebarMenuItem>
        </SidebarMenu>
    )
}
