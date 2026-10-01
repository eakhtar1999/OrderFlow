import Button from '@mui/material/Button';
import Card from '@mui/material/Card';
import CardActions from '@mui/material/CardActions';
import CardContent from '@mui/material/CardContent';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import { NavLink } from 'react-router-dom';

const LINKS = [
  {
    to: '/order/new',
    title: 'Place an order',
    description: 'Choose choreography or orchestration and watch the difference.',
  },
  {
    to: '/order/track',
    title: 'Track my orders',
    description: 'Search your orders by customer ID.',
  },
  {
    to: '/admin',
    title: 'Ops dashboard',
    description: 'Analytics, fraud lookups, and full order search.',
  },
];

export function HomePage() {
  return (
    <Stack spacing={3}>
      <Typography variant="h1">OrderFlow</Typography>
      <Typography color="text.secondary">
        A real-time order processing platform built on Kafka. Pick where to go.
      </Typography>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
        {LINKS.map((link) => (
          <Card key={link.to} variant="outlined" sx={{ flex: 1 }}>
            <CardContent>
              <Typography variant="h3">{link.title}</Typography>
              <Typography variant="body2" color="text.secondary">
                {link.description}
              </Typography>
            </CardContent>
            <CardActions>
              <Button component={NavLink} to={link.to} size="small">
                Go
              </Button>
            </CardActions>
          </Card>
        ))}
      </Stack>
    </Stack>
  );
}
