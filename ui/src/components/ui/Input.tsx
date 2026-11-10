import type { ComponentProps as ComponentPropsWithRef } from "preact";
import "./input.css";

export type InputProps = ComponentPropsWithRef<"input"> & {
  type?: "text" | "password";
  variant?: "default" | "embedded";
  class?: string | undefined;
};

export function Input({ variant = "default", class: classAttr, className, ...props }: InputProps) {
  const classes = ["ui-input", "ui-input--" + variant, classAttr, className].filter(Boolean).join(" ");
  return <input {...props} className={classes} />;
}
