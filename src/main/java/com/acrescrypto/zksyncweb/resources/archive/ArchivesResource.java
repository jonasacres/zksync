package com.acrescrypto.zksyncweb.resources.archive;

import java.io.IOException;
import java.util.ArrayList;

import javax.json.stream.JsonParsingException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XArchiveIdentification;
import com.acrescrypto.zksyncweb.data.XArchiveSpecification;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("archives")
public class ArchivesResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getArchives() {
		try {
			ArrayList<XArchiveIdentification> ids = new ArrayList<>();
			for(ZKArchiveConfig config : State.sharedState().getOpenConfigs()) {
				ids.add(XArchiveIdentification.fromConfig(config));
			}
			return XAPIResponse.withWrappedPayload("archives", ids);
		} catch(Exception exc) {
			return XAPIResponse.genericServerErrorResponse();
		}
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse postArchives(String json) {
		try {
			XArchiveSpecification spec = new ObjectMapper().readValue(json, XArchiveSpecification.class);
			ArchiveAccessor accessor;
			ZKArchiveConfig config;
			int status = 201;

			// build the accessor; this is the thing that lets us find archives with a matching read passphrase in the DHT.
			if(spec.getReadPassphrase() != null) {
				accessor = State.sharedState().getMaster().makeAccessorForPassphrase(spec.getReadPassphrase().getBytes());
			} else if(spec.getReadKey() != null) {
				Key key = new Key(State.sharedCrypto(), spec.getReadKey());
				accessor = State.sharedState().getMaster().makeAccessorForRoot(key, false);
			} else if(spec.getSeedKey() != null) {
				Key key = new Key(State.sharedCrypto(), spec.getSeedKey());
				accessor = State.sharedState().getMaster().makeAccessorForRoot(key, true);
			} else {
				return XAPIResponse.withError(400, "Must supply a read passphrase, read key or seed key");
			}

			// if we're doing a write key, set that up now 
			Key writeRoot = null;
			if(spec.getWritePassphrase() != null) {
				writeRoot = new Key(State.sharedCrypto(),
						State.sharedCrypto().deriveKeyFromPassphrase(spec.getWritePassphrase().getBytes(), CryptoSupport.PASSPHRASE_SALT_WRITE));
			} else if(spec.getWriteKey() != null) {
				writeRoot = new Key(State.sharedCrypto(), spec.getWriteKey()); 
			}

			if(spec.getArchiveId() == null) {
				// no archive id => we're making a new archive or joining a default archive
				if(writeRoot == null) {
					// no write root => anyone who knows the passphrase can write to this thing.
					if(spec.getDescription() == null && (spec.getPageSize() == null || spec.getPageSize() <= 0)) {
						/* default archive. these have an empty description and default page size.
						 * the archive root key is the same as the passphrase root, so the archive ID is
						 * deterministic. */
						config = ZKArchiveConfig.createDefault(accessor);
					} else {
						/* non-default archive, with non-deterministic archive ID. Future peers will
						 * need to know the archive ID to link up with us on this archive. */
						int pageSize = spec.getPageSize() == null ? ZKArchive.DEFAULT_PAGE_SIZE : spec.getPageSize();
						String description = spec.getDescription() == null ? "" : spec.getDescription();
						config = ZKArchiveConfig.create(accessor, description, pageSize);
					}

					ZKArchiveConfig existing = State.sharedState().configForArchiveId(config.getArchiveId());
					if(existing != null && existing != config) {
						status = 200;
						config = existing;
					}
				} else {
					/* write key supplied, so generate a random archive root key and make a new archive.
					 * future peers will need to know the archive ID to link up! */
					Key archiveRoot = new Key(State.sharedCrypto());
					config = ZKArchiveConfig.create(accessor, spec.getDescription(), spec.getPageSize(), archiveRoot, writeRoot);
				}
			} else {
				// joining an existing (likely non-default) archive
				ZKArchiveConfig existing = State.sharedState().configForArchiveId(spec.getArchiveId());
				if(existing != null) {
					status = 200;
					config = existing;
				} else {
					if(writeRoot == null) {
						config = ZKArchiveConfig.openExisting(accessor, spec.getArchiveId(), false, null);
					} else {
						config = ZKArchiveConfig.openExisting(accessor, spec.getArchiveId(), false, writeRoot);
					}

					/* We can't do anything with this archive until we have the config file, which we'll need to
					 * download from the swarm. So spin up a thread whose job is to get us swarmed up. */
					final ZKArchiveConfig cconfig = config;
					new Thread(() -> {
						try {
							cconfig.finishOpeningFromSwarm(0);
						} catch (IOException e) {}
					}).start();
				}
			}

			if(status == 201) {
				State.sharedState().addOpenConfig(config);
			}

			Integer level = spec.getSavedAccessLevel();
			if(level == null) level = StoredAccess.ACCESS_LEVEL_READWRITE;
			if(level != StoredAccess.ACCESS_LEVEL_NONE) {
				config.getMaster().storedAccess().storeArchiveAccess(config, level);
			}

			XArchiveIdentification id = XArchiveIdentification.fromConfig(config);
			throw XAPIResponse.withPayload(status, id);
		} catch(JsonParseException|JsonParsingException|JsonMappingException exc) {
			throw XAPIResponse.invalidJsonResponse();
		} catch(XAPIResponse resp) {
			throw resp;
		} catch(Exception exc) {
			exc.printStackTrace();

			throw XAPIResponse.genericServerErrorResponse();
		}
	}
}
