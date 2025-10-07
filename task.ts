import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';

// https://eagle-preprod.buddi.co.uk/index.html?action=help#help
const InputSchema = Type.Object({
    CustomerID: Type.String({
        description: 'The Customer ID provided by Buddi'
    }),
    RefreshToken: Type.String({
        description: 'The Refresh Token provided by Buddi'
    }),
    ClientSecret: Type.String({
        description: 'The Client Secret provided by Buddi'
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const OutputSchema = Type.Object({})

export default class Task extends ETL {
    static name = 'etl-buddi'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                return OutputSchema;
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const env = await this.env(InputSchema);

        const base = 'https://eagle-preprod.buddi.co.uk/apiv3/api';

        const authRes = await fetch(new URL(base + '/v1/token'), {
            method: 'GET',
            headers: {
                Accept: 'application/json',
                'User-Agent': 'CloudTAK ETL/1.0',
                'X-Client-Id': env.CustomerID,
                'X-Client-Secret': env.ClientSecret,
                'X-Refresh-Token': env.RefreshToken
            }
        });

        await authRes.json();

        const features: Static<typeof Feature.InputFeature>[] = [];

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features
        }

        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

