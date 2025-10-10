import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';

// https://eagle-preprod.buddi.co.uk/index.html?action=help#help
// https://eagle-preprod.buddi.co.uk/apidocs/
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
    MonitoredOnly: Type.Boolean({
        default: true,
        description: 'If true, only return monitored devices'
    }),
    Timeframe: Type.String({
        default: 'Last Day',
        enum: ['All', 'Last Day', 'Last 7 Days'],
    }),
    DEBUG: Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const OutputSchema = Type.Object({
    wearerId: Type.Integer(),
    firstName: Type.String(),
    lastName: Type.String(),
    lastGPSTime: Type.Union([Type.String(), Type.Null()]),
    lastGPSTimeInUTC: Type.Union([Type.String(), Type.Null()]),
    strapStatus: Type.Integer(),
    onCharge: Type.Integer(),
    gpsSignal: Type.Union([Type.Integer(), Type.Null()]),
    batteryPercentage: Type.Union([Type.Integer(), Type.Null()]),
    latitude: Type.Union([Type.Number(), Type.Null()]),
    longitude: Type.Union([Type.Number(), Type.Null()]),
    locationAddress: Type.Union([Type.String(), Type.Null()])
});

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

        const token = await authRes.typed(Type.Object({
            access_token: Type.String(),
            expires_at_utc: Type.String(),
            token_type: Type.String()
        }), {
            verbose: env.DEBUG || false
        });

        const trackerMap = new Map<string, Static<typeof Feature.InputFeature>>();

        let page = 1;
        let pages;

        do {
            const locURL = new URL(base + '/v1/wearers/locations');
            locURL.searchParams.set('page', String(page));
            locURL.searchParams.set('per_page', String(100))
            if (env.MonitoredOnly) {
                locURL.searchParams.set('monitored_only', 'true');
            }

            if (env.Timeframe && env.Timeframe !== 'All') {
                const date = new Date();
                if (env.Timeframe === 'Last Day') {
                    date.setDate(date.getDate() - 1);
                } else if (env.Timeframe === 'Last 7 Days') {
                    date.setDate(date.getDate() - 7);
                }

                locURL.searchParams.set('start_date', date.toISOString().split('T')[0]);
            }

            const locRes = await fetch(locURL, {
                method: 'GET',
                headers: {
                    Accept: 'application/json',
                    Authorization: `Buddi-oauthtoken: ${token.access_token}`,
                    'User-Agent': 'CloudTAK ETL/1.0',
                },
            });

            const trackers = await locRes.typed(Type.Object({
                result: Type.Integer(),
                code: Type.Optional(Type.Integer()),
                data: Type.Optional(Type.Array(OutputSchema)),
                message: Type.Optional(Type.String()),
                meta: Type.Optional(Type.Object({
                    total: Type.Integer(),
                    page: Type.Integer(),
                    per_page: Type.Integer(),
                    pages: Type.Integer()
                })),
                global_server_type: Type.Optional(Type.String()),
                global_server_code: Type.Optional(Type.String())
            }), {
                verbose: env.DEBUG || false
            });

            if (trackers.data) {
                for (const tracker of trackers.data) {
                    if (!tracker.lastGPSTimeInUTC || !tracker.latitude || !tracker.longitude) {
                        continue;
                    }

                    const id = `buddi-${tracker.wearerId}`;
                    const start = parseToISOUTC(tracker.lastGPSTimeInUTC);

                    const existing = trackerMap.get(id);

                    if (existing && new Date(existing.properties.start) > new Date(start)) {
                        continue;
                    }

                    const stale = new Date();
                    stale.setMinutes(stale.getMinutes() + 2);

                    trackerMap.set(id, {
                        id,
                        type: 'Feature',
                        properties: {
                            callsign: `Buddi: ${tracker.firstName} ${tracker.lastName}`,
                            type: 'a-h-G',
                            how: 'm-g',
                            start: start,
                            time: new Date().toISOString(),
                            stale: stale.toISOString(),
                            status: {
                                battery: String(tracker.batteryPercentage || 0),
                            },
                            metadata: tracker
                        },
                        geometry: {
                            type: 'Point',
                            coordinates: [tracker.longitude, tracker.latitude]
                        }
                    });
                }
            } else {
                console.log('no more data found, exiting paging');
                break;
            }

            if (!pages && trackers.meta.pages) {
                pages = trackers.meta.pages;
            }

            if (!pages) {
                console.log('could not determine number of pages');
                break;
            }

            ++page;
        } while (page <= pages);

        const features: Static<typeof Feature.InputFeature>[] = [];
        for (const feature of trackerMap.values()) {
            features.push(feature);
        }

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features
        }

        await this.submit(fc);
    }
}

/**
 * Parses a date string from 'MM/dd/yyyy hh:mm:ssaa' format to an ISO 8601 UTC string.
 * This function assumes the input time is to be interpreted as a UTC time.
 *
 * @param dateString The date string to parse, e.g., "10/25/2023 03:45:15PM".
 * @returns The ISO 8601 UTC string, e.g., "2023-10-25T15:45:15Z".
 * @throws An error if the input string does not match the expected format.
 */
function parseToISOUTC(dateString: string): string {
    // Groups: 1:MM, 2:dd, 3:yyyy, 4:hh, 5:mm, 6:ss, 7:aa (AM/PM)
    const regex = /^(\d{2})\/(\d{2})\/(\d{4}) (\d{1,2}):(\d{2}):(\d{2})(am|pm)$/i;

    const match = dateString.match(regex);

    if (!match) {
        throw new Error("Invalid date format. Expected 'MM/dd/yyyy hh:mm:ssaa'.");
    }

    const [, month, day, year, hour12Str, minute, second, ampm] = match;
    let hour = parseInt(hour12Str, 10);
    if (ampm.toUpperCase() === 'PM' && hour !== 12) {
        hour += 12;
    } else if (ampm.toUpperCase() === 'AM' && hour === 12) {
        hour = 0;
    }
    const hour24 = String(hour).padStart(2, '0');
    return `${year}-${month}-${day}T${hour24}:${minute}:${second}Z`;
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

