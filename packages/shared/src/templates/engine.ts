import { capitalizeWords, capitalizeFirst, formatDateShort, convertStringToEmoji, joinServices } from './helpers.js';

type FilterFn = (value: string, ...args: string[]) => string;

const FILTERS: Record<string, FilterFn> = {
  capitalize: (v) => capitalizeWords(v),
  capitalizeFirst: (v) => capitalizeFirst(v),
  formatDate: (v) => formatDateShort(v),
  joinServices: (v) => joinServices(v.split(',')),
};

/**
 * Render a template string with variables and optional filters.
 *
 * Syntax: {{variableName}} or {{variableName|filterName}}
 *
 * Example:
 *   "Olá {{nome|capitalize}}, temos um encontro em {{data|formatDate}}"
 *   with variables { nome: "ana maria", data: "2026-06-10" }
 *   → "Olá Ana Maria, temos um encontro em 10/jun (Quarta)"
 */
export function renderTemplate(template: string, variables: Record<string, string>): string {
  // First pass: emoji conversion
  let result = convertStringToEmoji(template);

  // Second pass: variable replacement with optional filters
  result = result.replace(/\{\{(\w+)(?:\|(\w+))?\}\}/g, (_match, varName: string, filterName: string | undefined) => {
    const value = variables[varName] ?? '';

    if (filterName && FILTERS[filterName]) {
      return FILTERS[filterName](value);
    }

    return value;
  });

  return result;
}

/**
 * Extract variable names from a template string.
 */
export function extractVariables(template: string): string[] {
  const matches = template.matchAll(/\{\{(\w+)(?:\|\w+)?\}\}/g);
  const vars = new Set<string>();
  for (const match of matches) {
    vars.add(match[1]);
  }
  return [...vars];
}
