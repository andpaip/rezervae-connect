/**
 * Capitalize all words: "ana maria" → "Ana Maria"
 */
export function capitalizeWords(str: string): string {
  return str
    .split(' ')
    .map((word) => {
      if (!word) return '';
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    })
    .join(' ');
}

/**
 * Capitalize first letter of each word (regex-based, handles multiple spaces).
 * "ana maria" → "Ana Maria"
 */
export function capitalizeFirst(str: string): string {
  return (str || '')
    .trim()
    .toLowerCase()
    .replace(/(?:^|\s)\S/g, (a) => a.toUpperCase());
}

/**
 * Format date string to Brazilian short format: "05/jan (Segunda)"
 */
export function formatDateShort(dateStr: string): string {
  const meses = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
  const diasSemana = ['Domingo', 'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado'];

  let date: Date;

  if (dateStr.includes('/')) {
    const [dia, mes, ano] = dateStr.split('/');
    date = new Date(`${ano}-${mes}-${dia}T00:00:00`);
  } else {
    date = new Date(dateStr + 'T00:00:00');
  }

  const dia = String(date.getDate()).padStart(2, '0');
  const mesAbrev = meses[date.getMonth()];
  const diaSemana = diasSemana[date.getDay()];

  return `${dia}/${mesAbrev} (${diaSemana})`;
}

/**
 * Convert emoji placeholders to actual emoji characters.
 * {coracao} → 💖, {rosa} → 🌹, etc.
 */
export function convertStringToEmoji(msg: string): string {
  const EMOJIS: Record<string, string> = {
    coracaoduplo: '💕',
    olharcoracao: '😍',
    beijocoracao: '😘',
    coracao: '💖',
    rosa: '🌹',
    triste: '😢',
  };

  let result = msg;
  for (const [key, emoji] of Object.entries(EMOJIS)) {
    result = result.replaceAll(`{${key}}`, emoji);
  }
  return result;
}

/**
 * Format promotion string: "Serviço1:descricao;Serviço2:descricao" → formatted
 */
export function formatPromocoes(message: string): string {
  if (!message) return '';
  return (
    'Aproveite as promoções que *separamos para agora:*\n\n' +
    message
      .split(';')
      .map((item) =>
        item.includes(':')
          ? `*${item.split(':')[0].trim()}*\n${item.split(':').slice(1).join(':').trim()}`
          : item,
      )
      .join('\n')
  );
}

/**
 * Join services with capitalized first letter.
 */
export function joinServices(servicos: string[]): string {
  return servicos.map((s) => s.charAt(0).toUpperCase() + s.slice(1)).join(', ');
}
