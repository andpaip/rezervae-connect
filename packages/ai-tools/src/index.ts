// AI Tools Registry - Phase 5C
// Stubs for future implementation

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: unknown;
  outputSchema: unknown;
  execute: (input: unknown, tenantId: string) => Promise<unknown>;
}

export class ToolRegistry {
  private tools = new Map<string, ToolDefinition>();

  register(tool: ToolDefinition): void {
    this.tools.set(tool.name, tool);
  }

  get(name: string): ToolDefinition | undefined {
    return this.tools.get(name);
  }

  list(): ToolDefinition[] {
    return Array.from(this.tools.values());
  }
}

export const toolRegistry = new ToolRegistry();
