/**
 * A minimal RESP client that speaks bytes, shared by the end-to-end suite and
 * the parity harness.
 *
 * `Bun.RedisClient` takes command arguments as strings and encodes them as
 * UTF-8, so it cannot express a value like `0xff` — that arrives as the two
 * bytes of U+00FF. Anything asserting on exact byte counts or exact byte
 * content has to bypass it, or it ends up measuring the client's encoder.
 */

export type RawRedisOptions = {
	host?: string;
	port?: number;
	/** Omitted or empty sends no AUTH, for a server that wants none. */
	password?: string;
};

export class RawRedis {
	private socket!: Awaited<ReturnType<typeof Bun.connect>>;
	private buffer = Buffer.alloc(0);
	private waiters: Array<(b: Buffer) => void> = [];

	async connect(opts: RawRedisOptions = {}) {
		const host = opts.host ?? process.env.REDIS_HOST ?? "localhost";
		const port = opts.port ?? Number(process.env.REDIS_PORT ?? 6379);
		const password = opts.password ?? process.env.REDIS_PASSWORD ?? "testpass";
		this.socket = await Bun.connect({
			hostname: host,
			port,
			socket: {
				data: (_s, chunk) => {
					this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
					this.drain();
				},
				error: () => {},
				close: () => {},
			},
		});
		if (password) await this.send("AUTH", password);
		return this;
	}

	/** Resolve waiters as soon as one complete reply is buffered. */
	private drain() {
		while (this.waiters.length > 0) {
			const end = replyEnd(this.buffer);
			if (end < 0) return;
			const reply = this.buffer.subarray(0, end);
			this.buffer = this.buffer.subarray(end);
			this.waiters.shift()?.(Buffer.from(reply));
		}
	}

	send(...args: Array<string | Buffer>): Promise<Buffer> {
		const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`)];
		for (const a of args) {
			const b = Buffer.isBuffer(a) ? a : Buffer.from(a, "utf8");
			parts.push(Buffer.from(`$${b.length}\r\n`), b, Buffer.from("\r\n"));
		}
		const done = new Promise<Buffer>((resolve) => this.waiters.push(resolve));
		this.socket.write(Buffer.concat(parts));
		return done;
	}

	/** The elements of a multi-bulk reply, with nil elements as null. */
	async array(...args: Array<string | Buffer>): Promise<Array<Buffer | null>> {
		const reply = await this.send(...args);
		if (reply[0] !== 0x2a) {
			throw new Error(`expected an array reply, got ${reply.subarray(0, 64)}`);
		}
		const first = reply.indexOf("\r\n") + 2;
		const count = Number(reply.subarray(1, first - 2).toString());
		const out: Array<Buffer | null> = [];
		let cur = first;
		for (let i = 0; i < count; i++) {
			const head = reply.indexOf("\r\n", cur);
			const len = Number(reply.subarray(cur + 1, head).toString());
			out.push(len < 0 ? null : reply.subarray(head + 2, head + 2 + len));
			cur = replyEnd(reply, cur);
		}
		return out;
	}

	/** The payload of a bulk-string reply, or null for a nil reply. */
	async bulk(...args: Array<string | Buffer>): Promise<Buffer | null> {
		const reply = await this.send(...args);
		if (reply[0] !== 0x24) return null; // not '$'
		const head = reply.indexOf("\r\n");
		const len = Number(reply.subarray(1, head).toString());
		if (len < 0) return null;
		return reply.subarray(head + 2, head + 2 + len);
	}

	close() {
		this.socket.end();
	}
}

/** Offset just past the complete RESP reply starting at `from` in `buf`, or -1. */
export function replyEnd(buf: Buffer, from = 0): number {
	if (from >= buf.length) return -1;
	const head = buf.indexOf("\r\n", from);
	if (head < 0) return -1;
	// An array is a header and then that many replies, each of which has to be
	// walked: stopping at the header would leave the elements in the buffer and
	// hand them to whatever command replies next.
	if (buf[from] === 0x2a) {
		const count = Number(buf.subarray(from + 1, head).toString());
		let cur = head + 2;
		for (let i = 0; i < count; i++) {
			cur = replyEnd(buf, cur);
			if (cur < 0) return -1;
		}
		return cur;
	}
	// Bulk strings carry a length; every other reply type ends at the newline.
	if (buf[from] !== 0x24) return head + 2;
	const len = Number(buf.subarray(from + 1, head).toString());
	if (len < 0) return head + 2;
	const end = head + 2 + len + 2;
	return buf.length >= end ? end : -1;
}

/**
 * The number an integer reply carries.
 *
 * Comparing `":2\r\n"` to a string asserts the wire format and the value at
 * once, and reads as neither. The wire format is pinned once, here.
 */
export function integerReply(reply: Buffer): number {
	const text = reply.toString();
	if (!text.startsWith(":"))
		throw new Error(`expected an integer, got ${text}`);
	return Number(text.slice(1, text.indexOf("\r\n")));
}

/** The message of an error reply, or null when the reply is not an error. */
export function errorReply(reply: Buffer): string | null {
	const text = reply.toString();
	return text.startsWith("-") ? text.slice(1, text.indexOf("\r\n")) : null;
}
