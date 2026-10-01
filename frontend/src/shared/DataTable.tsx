import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';

export interface DataTableColumn<Row> {
  key: string;
  header: string;
  render: (row: Row) => React.ReactNode;
}

interface DataTableProps<Row> {
  columns: DataTableColumn<Row>[];
  rows: Row[];
  getRowKey: (row: Row) => string;
  emptyMessage?: string;
}

/**
 * One generic table used everywhere a feature needs to show a list of
 * records (order-search's results, track-order's matches) — columns are
 * passed in as data, not hardcoded, so adding a new list view later means
 * defining a `DataTableColumn[]`, not writing a new `<table>` from
 * scratch. This is the "flexible design pattern" the ops dashboard's
 * tables actually lean on.
 */
export function DataTable<Row>({
  columns,
  rows,
  getRowKey,
  emptyMessage = 'No results.',
}: DataTableProps<Row>) {
  if (rows.length === 0) {
    return (
      <Box sx={{ py: 4, textAlign: 'center' }}>
        <Typography color="text.secondary">{emptyMessage}</Typography>
      </Box>
    );
  }

  return (
    <TableContainer component={Paper} variant="outlined">
      <Table size="small">
        <TableHead>
          <TableRow>
            {columns.map((column) => (
              <TableCell key={column.key}>{column.header}</TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={getRowKey(row)} hover>
              {columns.map((column) => (
                <TableCell key={column.key}>{column.render(row)}</TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}
