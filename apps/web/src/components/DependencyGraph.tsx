import type { GraphEdge, ServiceGraph } from "../api/incidents";

const COLUMN_WIDTH = 170;
const ROW_HEIGHT = 56;
const NODE_WIDTH = 130;
const NODE_HEIGHT = 34;

interface Placed {
  id: string;
  name: string;
  x: number;
  y: number;
}

/** Column per signed hop: dependents left of the service, dependencies right of it. */
function depthByService(graph: ServiceGraph): Map<string, number> {
  const depth = new Map([[graph.service_id, 0]]);
  const place = (id: string, value: number) => {
    const known = depth.get(id);
    if (known === undefined || Math.abs(value) < Math.abs(known)) {
      depth.set(id, value);
    }
  };
  graph.downstream.forEach((edge) => {
    place(edge.to_service_id, edge.hops);
  });
  graph.upstream.forEach((edge) => {
    place(edge.from_service_id, -edge.hops);
  });
  return depth;
}

function layout(graph: ServiceGraph): { nodes: Map<string, Placed>; width: number; height: number } {
  const depth = depthByService(graph);
  const columns = new Map<number, string[]>();
  depth.forEach((value, id) => {
    columns.set(value, [...(columns.get(value) ?? []), id]);
  });
  const ordered = [...columns.keys()].sort((a, b) => a - b);
  const rows = Math.max(...[...columns.values()].map((ids) => ids.length));
  const names = new Map(graph.services.map((service) => [service.service_id, service.name]));

  const nodes = new Map<string, Placed>();
  ordered.forEach((column, columnIndex) => {
    const ids = columns.get(column) ?? [];
    const offset = ((rows - ids.length) * ROW_HEIGHT) / 2;
    ids.forEach((id, rowIndex) => {
      nodes.set(id, {
        id,
        name: names.get(id) ?? id,
        x: columnIndex * COLUMN_WIDTH + (COLUMN_WIDTH - NODE_WIDTH) / 2,
        y: offset + rowIndex * ROW_HEIGHT + (ROW_HEIGHT - NODE_HEIGHT) / 2,
      });
    });
  });
  return { nodes, width: ordered.length * COLUMN_WIDTH, height: rows * ROW_HEIGHT };
}

function uniqueEdges(graph: ServiceGraph): GraphEdge[] {
  const edges = new Map<string, GraphEdge>();
  [...graph.downstream, ...graph.upstream].forEach((edge) => {
    edges.set(`${edge.from_service_id}:${edge.to_service_id}`, edge);
  });
  return [...edges.values()];
}

interface DependencyGraphProps {
  graph: ServiceGraph;
  /** Services the incident names; the drawing marks them. */
  affected: ReadonlySet<string>;
}

export function DependencyGraph({ graph, affected }: DependencyGraphProps) {
  const { nodes, width, height } = layout(graph);
  const edges = uniqueEdges(graph);
  const label = (id: string) => nodes.get(id)?.name ?? id;

  return (
    <>
      <div className="scroll-x">
        <svg
          className="graph"
          role="img"
          aria-label={`Dependency neighborhood of ${label(graph.service_id)}`}
          width={width}
          height={height}
          viewBox={`0 0 ${String(width)} ${String(height)}`}
        >
          <defs>
            <marker
              id="graph-arrow"
              viewBox="0 0 8 8"
              refX="8"
              refY="4"
              markerWidth="7"
              markerHeight="7"
              orient="auto"
            >
              <path d="M0 0 L8 4 L0 8 z" className="graph-arrow" />
            </marker>
          </defs>
          {edges.map((edge) => {
            const from = nodes.get(edge.from_service_id);
            const to = nodes.get(edge.to_service_id);
            if (!from || !to) {
              return null;
            }
            const forward = from.x <= to.x;
            return (
              <line
                key={`${edge.from_service_id}:${edge.to_service_id}`}
                className="graph-edge"
                x1={forward ? from.x + NODE_WIDTH : from.x}
                y1={from.y + NODE_HEIGHT / 2}
                x2={forward ? to.x : to.x + NODE_WIDTH}
                y2={to.y + NODE_HEIGHT / 2}
                markerEnd="url(#graph-arrow)"
              />
            );
          })}
          {[...nodes.values()].map((node) => (
            <g key={node.id}>
              <rect
                className={affected.has(node.id) ? "graph-node graph-node-affected" : "graph-node"}
                x={node.x}
                y={node.y}
                width={NODE_WIDTH}
                height={NODE_HEIGHT}
                rx={6}
              />
              <text
                className="graph-label"
                x={node.x + NODE_WIDTH / 2}
                y={node.y + NODE_HEIGHT / 2}
                textAnchor="middle"
                dominantBaseline="central"
              >
                {node.name}
              </text>
            </g>
          ))}
        </svg>
      </div>
      <div className="scroll-x">
        <table>
          <caption>Observed dependencies as of the incident</caption>
          <thead>
            <tr>
              <th scope="col">Caller</th>
              <th scope="col">Dependency</th>
              <th scope="col">Hops</th>
            </tr>
          </thead>
          <tbody>
            {edges.map((edge) => (
              <tr key={`${edge.from_service_id}:${edge.to_service_id}`}>
                <td>{label(edge.from_service_id)}</td>
                <td>{label(edge.to_service_id)}</td>
                <td>{edge.hops}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
