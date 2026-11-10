import type { InputHTMLAttributes } from "preact";
import "./checkbox.css";

export type CheckboxProps = Omit<InputHTMLAttributes<HTMLInputElement>, "type" | "role"> & { class?: string | undefined };

export function Checkbox({ class: classAttr, className, ...props }: CheckboxProps) {
  const classes = ["ui-checkbox", classAttr, className].filter(Boolean).join(" ");
  return <input {...props} type="checkbox" className={classes} />;
}
