import type { ComponentProps as ComponentPropsWithRef } from "preact";
import "./button.css";

export type ButtonProps = ComponentPropsWithRef<"button"> & {
  variant?: "primary" | "secondary";
  class?: string | undefined;
};

export function Button({
  variant = "secondary",
  type = "button",
  class: classAttr,
  className,
  ...props
}: ButtonProps) {
  const classes = ["ui-button", "ui-button--" + variant, classAttr, className].filter(Boolean).join(" ");
  return <button {...props} type={type} className={classes} />;
}
