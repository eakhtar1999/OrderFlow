import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';

interface StatCardProps {
  label: string;
  value: string;
  hint?: string;
}

/** Reused across the Overview and Analytics pages — a single number with
 * a label, nothing fancier. */
export function StatCard({ label, value, hint }: StatCardProps) {
  return (
    <Card variant="outlined" sx={{ minWidth: 200 }}>
      <CardContent>
        <Stack spacing={0.5}>
          <Typography variant="body2" color="text.secondary">
            {label}
          </Typography>
          <Typography variant="h2">{value}</Typography>
          {hint && (
            <Typography variant="caption" color="text.secondary">
              {hint}
            </Typography>
          )}
        </Stack>
      </CardContent>
    </Card>
  );
}
