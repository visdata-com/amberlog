import type { ComponentPropsWithRef } from "react";
import "./checkbox.css";

export type CheckboxProps = Omit<ComponentPropsWithRef<"input">, "type"> & { class?: string | undefined };

export function Checkbox({ class: classAttr, className, ...props }: CheckboxProps) {
  const classes = ["ui-checkbox", classAttr, className].filter(Boolean).join(" ");
  return <input {...props} type="checkbox" className={classes} />;
}
