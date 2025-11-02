import { Link, useNavigate } from '@tanstack/react-router'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Command, LogOutIcon, UserCircleIcon } from 'lucide-react'
import { signOut } from "@hono/auth-js/react"

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

import { useCurrentSession } from "@/hooks/use-current-session"

export default function Header() {
  const navigate = useNavigate()
  const { data: session } = useCurrentSession()
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
    <>
      <header className="p-4 lg:px-14 border">
        <div className="max-w-screen-2xl mx-auto">
          <div className="w-full flex items-center justify-between">
            <div className="flex items-center lg:gap-x-16">
              <Link to="/">
                <div className="items-center flex">
                  <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
                    <Command className="size-4" />
                  </div>
                  <div className="grid flex-1 text-left text-sm leading-tight ml-2.5">
                    <span className="truncate font-semibold">PUMA</span>
                    <span className="truncate text-xs">Enterprise</span>
                  </div>
                </div>
              </Link>
            </div>

            <DropdownMenu>
              <DropdownMenuTrigger className="cursor-pointer rounded-full">
                <Avatar className="h-10 w-10 rounded-full grayscale">
                  <AvatarImage src='https://github.com/shadcn.png' alt={session?.user?.name || 'FC'} />
                  <AvatarFallback className="rounded-full">{session?.user?.name ? session.user.name.split(' ')[0] : 'FC'}</AvatarFallback>
                </Avatar>
              </DropdownMenuTrigger>
              <DropdownMenuContent
                className="w-[--radix-dropdown-menu-trigger-width] min-w-56 rounded-lg"
                align="end"
                sideOffset={4}
              >
                <DropdownMenuLabel className="p-0 font-normal">
                  <div className="flex items-center gap-2 px-1 py-1.5 text-left text-sm">
                    <Avatar className="h-8 w-8 rounded-lg">
                      <AvatarImage src='https://github.com/shadcn.png' alt={session?.user?.name || 'FC'} />
                      <AvatarFallback className="rounded-lg">CN</AvatarFallback>
                    </Avatar>
                    <div className="grid flex-1 text-left text-sm leading-tight">
                      <span className="truncate font-medium">{session?.user?.name}</span>
                      <span className="truncate text-xs text-muted-foreground">
                        {session?.user?.email}
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
          </div>
        </div>
      </header>
    </>
  )
}
