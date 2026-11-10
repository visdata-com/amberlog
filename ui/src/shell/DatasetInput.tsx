// 数据集输入框：统一候选列表、无浏览器自动填充和键盘提交行为。

import type { JSX } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

export interface DatasetInputProps {
  id: string;
  listId: string;
  value: string;
  options: readonly string[];
  placeholder?: string;
  onInput: (value: string, inputType: string | undefined) => void;
  onBlur?: (value: string) => void;
  onEnter?: (value: string) => void;
}

export function DatasetInput({ id, listId, value, options, placeholder = "输入或选择数据集", onInput, onBlur, onEnter }: DatasetInputProps): JSX.Element {
  const rootRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const needle = value.trim().toLowerCase();
  const matches = options.filter((option) => needle === "" || option.toLowerCase().includes(needle));
  const visible = matches.slice(0, 200);
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
    setActiveIndex(0);
  }, [value]);

  const choose = (option: string): void => {
    onInput(option, "insertReplacementText");
    setOpen(false);
    inputRef.current?.focus();
  };

  const onKeyDown = (event: KeyboardEvent): void => {
    switch (event.key) {
      case "ArrowDown":
        if (visible.length === 0) return;
        event.preventDefault();
        setOpen(true);
        setActiveIndex((index) => Math.min(index + 1, visible.length - 1));
        break;
      case "ArrowUp":
        if (visible.length === 0) return;
        event.preventDefault();
        setOpen(true);
        setActiveIndex((index) => Math.max(index - 1, 0));
        break;
      case "Enter":
        if (open && visible[activeIndex] !== undefined) {
          event.preventDefault();
          choose(visible[activeIndex]);
        } else if (onEnter !== undefined) {
          onEnter(inputRef.current?.value ?? value);
        }
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
    <div ref={rootRef} class="dataset-input custom-select">
      <input
        ref={inputRef}
        id={id}
        class="dataset-input__input"
        autocomplete="off"
        placeholder={placeholder}
        aria-label="数据集"
        role="combobox"
        aria-autocomplete="list"
        aria-expanded={open}
        aria-controls={listboxId}
        aria-activedescendant={open && visible[activeIndex] !== undefined ? `${id}-option-${activeIndex}` : undefined}
        value={value}
        onFocus={() => setOpen(visible.length > 0)}
        onClick={() => setOpen(visible.length > 0)}
        onInput={(event) => {
          onInput(event.currentTarget.value, (event as InputEvent).inputType);
          setOpen(true);
        }}
        onBlur={onBlur === undefined ? undefined : (event) => onBlur(event.currentTarget.value)}
        onKeyDown={onKeyDown}
      />
      <span class="custom-select__chevron" aria-hidden="true" />
      {open && visible.length > 0 && (
        <div id={listboxId} class="custom-select__menu dataset-input__menu" role="listbox" aria-label="数据集候选">
          {visible.map((option, index) => (
            <div
              id={`${id}-option-${index}`}
              class="custom-select__option"
              role="option"
              aria-selected={option === value}
              data-active={index === activeIndex}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => choose(option)}
            >
              <span>{option}</span>
              {option === value && <span class="custom-select__check" aria-hidden="true">✓</span>}
            </div>
          ))}
        </div>
      )}
      <datalist id={listId} hidden>
        {options.map((option) => (
          <option value={option} />
        ))}
      </datalist>
    </div>
  );
}
