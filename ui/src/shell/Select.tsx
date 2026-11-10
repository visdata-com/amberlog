// 轻量自定义下拉：保留 combobox/listbox 语义，不依赖原生 select 的浏览器样式。

import type { JSX } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

export interface SelectOption {
  value: string;
  label: string;
}

export interface SelectProps {
  id: string;
  value: string;
  options: readonly SelectOption[];
  onChange: (value: string) => void;
  "aria-label"?: string;
  class?: string;
  disabled?: boolean;
  searchable?: boolean;
  placeholder?: string;
}

export function Select({ id, value, options, onChange, "aria-label": ariaLabel, class: className, disabled = false, searchable = false, placeholder = "请选择" }: SelectProps): JSX.Element {
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const needle = query.trim().toLowerCase();
  const visible = searchable && open && needle !== ""
    ? options.filter((option) => option.label.toLowerCase().includes(needle) || option.value.toLowerCase().includes(needle))
    : options;
  const selectedIndex = Math.max(0, visible.findIndex((option) => option.value === value));
  const [activeIndex, setActiveIndex] = useState(selectedIndex);
  const selected = options.find((option) => option.value === value);
  const listboxId = id + "-listbox";

  useEffect(() => {
    if (!open) return undefined;
    const close = (event: MouseEvent): void => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const option = document.getElementById(`${id}-option-${activeIndex}`);
    option?.scrollIntoView({ block: "nearest" });
  }, [activeIndex, id, open, needle]);

  const move = (step: number): void => {
    if (visible.length === 0) return;
    setActiveIndex((index) => (index + step + visible.length) % visible.length);
  };

  const choose = (index: number): void => {
    const option = visible[index];
    if (option === undefined) return;
    onChange(option.value);
    if (searchable) inputRef.current?.focus();
    else triggerRef.current?.focus();
    setActiveIndex(index);
    setOpen(false);
  };

  const show = (): void => {
    setQuery("");
    setActiveIndex(Math.max(0, options.findIndex((option) => option.value === value)));
    setOpen(true);
  };

  const onKeyDown = (event: KeyboardEvent): void => {
    if (event.isComposing) return;
    switch (event.key) {
      case "ArrowDown":
        event.preventDefault();
        if (!open) {
          show();
        } else move(1);
        break;
      case "ArrowUp":
        event.preventDefault();
        if (!open) {
          show();
        } else move(-1);
        break;
      case "Home":
        if (open && !searchable) {
          event.preventDefault();
          setActiveIndex(0);
        }
        break;
      case "End":
        if (open && !searchable) {
          event.preventDefault();
          setActiveIndex(Math.max(0, visible.length - 1));
        }
        break;
      case "Enter":
      case " ":
        if (event.key === " " && searchable) return;
        event.preventDefault();
        if (open) choose(activeIndex);
        else show();
        break;
      case "Escape":
        if (open) {
          event.preventDefault();
          setOpen(false);
        }
        break;
      case "Tab":
        setOpen(false);
        break;
    }
  };

  return (
    <div ref={rootRef} class={`custom-select${searchable ? " custom-select--searchable" : ""}${className === undefined ? "" : ` ${className}`}`}>
      {searchable ? (
        <>
          <input
            ref={inputRef}
            id={id}
            class="custom-select__input"
            role="combobox"
            aria-label={ariaLabel}
            aria-autocomplete="list"
            aria-haspopup="listbox"
            aria-expanded={open}
            aria-controls={listboxId}
            aria-activedescendant={open && visible[activeIndex] !== undefined ? `${id}-option-${activeIndex}` : undefined}
            autocomplete="off"
            spellcheck={false}
            disabled={disabled}
            placeholder={placeholder}
            value={open ? query : selected?.label ?? value}
            onFocus={show}
            onClick={() => { if (!open) show(); }}
            onInput={(event) => {
              setQuery(event.currentTarget.value);
              setActiveIndex(0);
              setOpen(true);
            }}
            onBlur={() => setOpen(false)}
            onKeyDown={onKeyDown}
          />
          <span class="custom-select__chevron" aria-hidden="true" />
        </>
      ) : (
        <button
          ref={triggerRef}
          id={id}
          type="button"
          class="custom-select__trigger"
          value={value}
          role="combobox"
          aria-label={ariaLabel}
          aria-haspopup="listbox"
          aria-expanded={open}
          aria-controls={listboxId}
          aria-activedescendant={open && visible[activeIndex] !== undefined ? `${id}-option-${activeIndex}` : undefined}
          disabled={disabled}
          onClick={() => {
            setActiveIndex(selectedIndex);
            setOpen((shown) => !shown);
          }}
          onKeyDown={onKeyDown}
        >
          <span class="custom-select__value" data-placeholder={selected === undefined && value === ""}>
            {selected?.label ?? (value || placeholder)}
          </span>
          <span class="custom-select__chevron" aria-hidden="true" />
        </button>
      )}
      {open && (
        <div id={listboxId} class="custom-select__menu" role="listbox" aria-label={ariaLabel}>
          {visible.length === 0 && <div class="custom-select__empty" role="status">无匹配选项</div>}
          {visible.map((option, index) => (
            <div
              key={option.value}
              id={`${id}-option-${index}`}
              class="custom-select__option"
              role="option"
              aria-selected={option.value === value}
              data-active={index === activeIndex}
              data-value={option.value}
              onMouseDown={(event) => event.preventDefault()}
              onClick={(event) => {
                event.preventDefault();
                choose(index);
              }}
            >
              <span>{option.label}</span>
              {option.value === value && <span class="custom-select__check" aria-hidden="true">✓</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
