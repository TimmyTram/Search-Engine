import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

import express from 'express';
import searchRoutes from './routes/search.route';
import healthRoutes from './routes/health.route';

const app = express();
const port = process.env.QUERY_ENGINE_PORT || 5000;


app.use(express.json());

app.use('/api/v1/health', healthRoutes);
app.use('/api/v1/search', searchRoutes);

app.listen(port, () => {
    console.log(`Query Engine is Live at: http://localhost:${port}`);
});
