import 'dotenv/config';
import { db } from './db.js';
import { tenants, whatsappInstances, tenantFeatures } from './schema/index.js';

async function seed() {
  console.log('Seeding database...');

  // Tenant
  const [tenant] = await db.insert(tenants).values({
    name: 'Rezervae',
    slug: 'rezervae',
    settings: { rateLimitPerDay: 5000, plan: 'premium' },
    status: 'active',
  }).returning();

  console.log(`Created tenant: ${tenant.name} (${tenant.id})`);

  // Instances
  const instances = await db.insert(whatsappInstances).values([
    { tenantId: tenant.id, instanceName: 'receive', sessionName: `${tenant.slug}-receive`, provider: 'wppconnect' },
    { tenantId: tenant.id, instanceName: 'send', sessionName: `${tenant.slug}-send`, provider: 'wppconnect' },
    { tenantId: tenant.id, instanceName: 'campaigns', sessionName: `${tenant.slug}-campaigns`, provider: 'wppconnect' },
  ]).returning();

  console.log(`Created ${instances.length} instances`);

  // Feature flags
  const features = await db.insert(tenantFeatures).values([
    { tenantId: tenant.id, feature: 'ai_enabled', enabled: false },
    { tenantId: tenant.id, feature: 'campaigns_enabled', enabled: true },
    { tenantId: tenant.id, feature: 'inbox_enabled', enabled: false },
    { tenantId: tenant.id, feature: 'voice_enabled', enabled: false },
    { tenantId: tenant.id, feature: 'instagram_enabled', enabled: false },
    { tenantId: tenant.id, feature: 'analytics_enabled', enabled: false },
  ]).returning();

  console.log(`Created ${features.length} feature flags`);

  console.log('Seed complete!');
  process.exit(0);
}

seed().catch((err) => {
  console.error('Seed failed:', err);
  process.exit(1);
});
