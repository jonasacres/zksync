package com.acrescrypto.zksyncweb.resources.archive;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebUtils;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XArchiveSettings;
import com.acrescrypto.zksyncweb.data.XArchiveIdentification;
import com.acrescrypto.zksyncweb.data.XArchiveSpecification;
import com.acrescrypto.zksyncweb.data.XRevisionInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/archives/{archiveId}")
public class ArchiveResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getArchive(@PathParam("archiveId") String archiveId) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		throw XAPIResponse.withPayload(XArchiveIdentification.fromConfig(config));
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse deleteArchive(@PathParam("archiveId") String archiveId) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		config.close();
		config.getCacheStorage().purge();
		config.getLocalStorage().purge();
		State.sharedState().removeConfig(config);
		State.sharedState().getMaster().storedAccess().deleteArchiveAccess(config);
		throw XAPIResponse.withPayload(XArchiveIdentification.fromConfig(config));
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/seed")
	public XAPIResponse getSeed(@PathParam("archiveId") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setSeedKey(config.getAccessor().getSeedRoot().getRaw());
		spec.setArchiveId(config.getArchiveId());
		spec.setSavedAccessLevel(StoredAccess.ACCESS_LEVEL_SEED);
		
		throw XAPIResponse.withPayload(spec);
	}
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/keys")
	public XAPIResponse putKeys(@PathParam("archiveId") String archiveId, String json) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		XArchiveSpecification spec = new ObjectMapper().readValue(json, XArchiveSpecification.class);
		
		if(spec.getReadPassphrase() != null) {
			byte[] passphraseRoot = State.sharedCrypto().deriveKeyFromPassphrase(spec.getReadPassphrase().getBytes(), CryptoSupport.PASSPHRASE_SALT_READ);
			config.getAccessor().setPassphraseRoot(new Key(State.sharedCrypto(), passphraseRoot));
		}
		
		if(spec.getReadKey() != null) {
			config.getAccessor().setPassphraseRoot(new Key(State.sharedCrypto(), spec.getReadKey()));
		}
		
		if(spec.getWritePassphrase() != null) {
			byte[] writeRoot = State.sharedCrypto().deriveKeyFromPassphrase(spec.getWritePassphrase().getBytes(), CryptoSupport.PASSPHRASE_SALT_WRITE);
			config.setWriteRoot(new Key(State.sharedCrypto(), writeRoot));
		}

		if(spec.getWriteKey() != null) {
			config.setWriteRoot(new Key(State.sharedCrypto(), spec.getWriteKey()));
		}
		
		// Ignore seed key, since we 404 if we deleted archive access anyway.
		
		WebUtils.mapFieldWithException(spec.getSavedAccessLevel(), (level)->{
			if(level > StoredAccess.ACCESS_LEVEL_NONE ) {
				config.getMaster().storedAccess().storeArchiveAccess(config, spec.getSavedAccessLevel());
			} else if(level == StoredAccess.ACCESS_LEVEL_NONE) {
				State.sharedState().getMaster().storedAccess().deleteArchiveAccess(config);
			}
			
			// ignore negative access levels for now
		});
		
		throw XAPIResponse.successResponse();
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/keys/write")
	public XAPIResponse deleteKeysWrite(@PathParam("archiveId") String archiveId) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		config.clearWriteRoot();
		config.getMaster().storedAccess().storeArchiveAccess(config, StoredAccess.ACCESS_LEVEL_READ);
		
		throw XAPIResponse.successResponse();
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/keys/read")
	public XAPIResponse deleteKeysRead(@PathParam("archiveId") String archiveId) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		config.clearWriteRoot();
		config.getAccessor().becomeSeedOnly();
		config.getMaster().storedAccess().storeArchiveAccess(config, StoredAccess.ACCESS_LEVEL_SEED);
		
		throw XAPIResponse.successResponse();
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/keys")
	public XAPIResponse deleteKeysSeed(@PathParam("archiveId") String archiveId) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		config.clearWriteRoot();
		config.getAccessor().becomeSeedOnly();
		config.getMaster().storedAccess().deleteArchiveAccess(config);
		
		throw XAPIResponse.successResponse();
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/commit")
	public XAPIResponse postCommit(@PathParam("archiveId") String archiveId) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		RevisionTag tag = State.sharedState().activeFs(config).commit();
		return XAPIResponse.withPayload(new XRevisionInfo(tag, 1));
	}
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/title")
	public XAPIResponse putTitle(@PathParam("archiveId") String archiveId, String title) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		State.sharedState().activeFs(config).getInodeTable().setNextTitle(title);
		throw XAPIResponse.successResponse();
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/title")
	public XAPIResponse getTitle(@PathParam("archiveId") String archiveId) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		String title = State.sharedState().activeFs(config).getInodeTable().getNextTitle();
		throw XAPIResponse.withWrappedPayload("title", title);
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/settings")
	public XAPIResponse getSettings(@PathParam("archiveId") String archiveId) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		throw XAPIResponse.withPayload(XArchiveSettings.fromConfig(config));
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/discover")
	public XAPIResponse postDiscover(@PathParam("archiveId") String archiveId) throws XAPIResponse, IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		config.getMaster().getDHTDiscovery().forceUpdate(config.getAccessor());
		throw XAPIResponse.successResponse();
	}
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/settings")
	public XAPIResponse putSettings(@PathParam("archiveId") String archiveId, String json) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		XArchiveSettings settings = new ObjectMapper().readValue(json, XArchiveSettings.class);
		
		WebUtils.mapField(settings.isAdvertising(),
				()->config.advertise(),
				()->config.stopAdvertising());
		WebUtils.mapField(settings.isRequestingAll(),
				()->config.getSwarm().requestAll(),
				()->config.getSwarm().stopRequestingAll());
		WebUtils.mapField(settings.getPeerLimit(),
				(limit)->config.getSwarm().setMaxSocketCount(limit));
		
		if(!config.getAccessor().isSeedOnly()) {
			WebUtils.mapFieldWithException(settings.getAutocommitInterval(),
					(interval)->{
						State.sharedState().activeManager(config).setAutocommitIntervalMs(interval);
					});
			WebUtils.mapFieldWithException(settings.isAutocommit(),
					(autocommit)->{
						State.sharedState().activeManager(config).setAutocommit(autocommit);
					});
			WebUtils.mapFieldWithException(settings.isAutomerge(),
					(automerge)->{
						State.sharedState().activeManager(config).setAutomerge(settings.isAutomerge());
					});
			WebUtils.mapFieldWithException(settings.isAutofollow(),
					(autofollow)->{
						State.sharedState().activeManager(config).setAutofollow(autofollow);
					});
			WebUtils.mapFieldWithException(settings.getAutomirrorPath(),
					(automirrorPath)->{
						/* There's a gotcha here: automirrorPath = null doesn't do what we might expect
						 * from the ZKFSManager contract, because the convention in the API is to
						 * use null/omission to indicate that we don't want to change the value,
						 * whereas in the contract in means that we want to clear the setting.
						 * 
						 * So we allow the caller to clear our automirrorPath by setting it to the
						 * empty string, which is unlikely to ever be what the user intends anyway.
						 */
						// TODO Someday: Permissions on this
						/* When we move to permissions, automirror is in general SUPER DANGEROUS.
						 * We can get the API server to manipulate the local filesystem. So we want
						 * to be able to:
						 *   1) restrict automirror controls to certain people,
						 *   2) restrict all automirrors to a certain directory, and
						 *   3) disable the feature entirely.
						 */
						if(automirrorPath.equals("")) {
							automirrorPath = null;
						} else {
							try(LocalFS fs = new LocalFS("/")) {
								if(!fs.exists(automirrorPath)) {
									throw XAPIResponse.withError(409, "requested automirror path not found");
								}
								
								if(!fs.stat(automirrorPath).isDirectory()) {
									throw XAPIResponse.withError(409, "requested automirror path is not directory");
								}
							}
						}
						
						State
							.sharedState()
							.activeManager(config)
							.setAutomirrorPath(automirrorPath);
					});
			WebUtils.mapFieldWithException(settings.isAutomirror(),
					(automirror)->{
						if(State.sharedState().activeManager(config).getAutomirrorPath() == null) {
							throw XAPIResponse.withError(400, "must set automirror path to enable automirroring");
						}
						
						try {
							State.sharedState().activeManager(config).setAutomirror(automirror);
						} catch(ENOENTException|NoSuchFileException exc) {
							throw XAPIResponse.withError(409, "requested automirror path not found");
						}
					});
		}
		
		throw XAPIResponse.successResponse();
	}
}
