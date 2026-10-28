import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "./select";

const HOURS = Array.from({ length: 24 }, (_, hour) => {
  const value = String(hour).padStart(2, "0");
  return { value, label: value };
});
const MINUTES = Array.from({ length: 60 }, (_, minute) => {
  const value = String(minute).padStart(2, "0");
  return { value, label: value };
});

export function TimePicker({
  id,
  value,
  onValueChange,
  size = "default",
}: {
  id?: string;
  value: string;
  onValueChange: (value: string) => void;
  size?: "default" | "sm";
}) {
  const [hour, minute] = value.split(":");
  return (
    <div role="group" aria-label="Time" className="flex min-w-0 items-center gap-1.5">
      <Select
        value={hour}
        items={HOURS}
        onValueChange={(next) => {
          if (next !== null) onValueChange(`${next}:${minute}`);
        }}
      >
        <SelectTrigger id={id} size={size} className="min-w-0 flex-1" aria-label="Hours">
          <SelectValue />
        </SelectTrigger>
        <SelectPopup alignItemWithTrigger={false} variant="compact">
          {HOURS.map(({ value, label }) => (
            <SelectItem key={value} value={value}>
              {label}
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
      <span aria-hidden className="text-muted-foreground">
        :
      </span>
      <Select
        value={minute}
        items={MINUTES}
        onValueChange={(next) => {
          if (next !== null) onValueChange(`${hour}:${next}`);
        }}
      >
        <SelectTrigger size={size} className="min-w-0 flex-1" aria-label="Minutes">
          <SelectValue />
        </SelectTrigger>
        <SelectPopup alignItemWithTrigger={false} variant="compact">
          {MINUTES.map(({ value, label }) => (
            <SelectItem key={value} value={value}>
              {label}
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
    </div>
  );
}
