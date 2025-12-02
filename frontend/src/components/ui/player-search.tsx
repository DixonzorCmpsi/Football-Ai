"use client"

import * as React from "react"
import { Check, ChevronsUpDown } from "lucide-react"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"

export function PlayerSearch({ players, onSelect }: { players: any[], onSelect: (name: string) => void }) {
  const [open, setOpen] = React.useState(false)
  const [value, setValue] = React.useState("")

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between"
        >
          {value ? players.find((player) => player.player_name === value)?.player_name : "Select player..."}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[300px] p-0">
        <Command>
          <CommandInput placeholder="Search player..." />
          <CommandList>
            <CommandEmpty>No player found.</CommandEmpty>
            <CommandGroup>
              {players.slice(0, 50).map((player) => ( // Limit list for performance
                <CommandItem
                  key={player.player_name + player.team_abbr}
                  value={player.player_name}
                  onSelect={(currentValue) => {
                    setValue(currentValue === value ? "" : currentValue)
                    onSelect(currentValue)
                    setOpen(false)
                  }}
                >
                  <Check
                    className={cn(
                      "mr-2 h-4 w-4",
                      value === player.player_name ? "opacity-100" : "opacity-0"
                    )}
                  />
                  {player.player_name} <span className="ml-2 text-xs text-muted-foreground">({player.position} - {player.team_abbr})</span>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}