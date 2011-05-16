package com.progress.codeshare.esbservice.ruleEngine;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.rules.Handle;
import javax.rules.RuleRuntime;
import javax.rules.RuleServiceProviderManager;
import javax.rules.StatefulRuleSession;
import javax.rules.admin.LocalRuleExecutionSetProvider;
import javax.rules.admin.RuleAdministrator;
import javax.rules.admin.RuleExecutionSet;

import org.drools.jsr94.rules.Constants;
import org.drools.jsr94.rules.RuleServiceProviderImpl;
import org.drools.jsr94.rules.admin.RuleExecutionSetRepository;

import com.sonicsw.xq.XQConstants;
import com.sonicsw.xq.XQEnvelope;
import com.sonicsw.xq.XQInitContext;
import com.sonicsw.xq.XQParameters;
import com.sonicsw.xq.XQService;
import com.sonicsw.xq.XQServiceContext;
import com.sonicsw.xq.XQServiceException;

public final class RuleEngineService implements XQService {
	private static final String MODE_FILE = "File";

	private static final String MODE_REPOSITORY = "Repository";

	private static final String PARAM_NAME_DRL_FILE = "drlFile";

	private static final String PARAM_NAME_DSL_FILE = "dslFile";

	private static final String PARAM_NAME_MODE = "mode";

	private static final String PARAM_NAME_URI = "uri";

	public void destroy() {
	}

	public void init(XQInitContext initCtx) {
	}

	public void service(final XQServiceContext servCtx)
			throws XQServiceException {

		try {

			while (servCtx.hasNextIncoming()) {
				/* Set the rule engine up */
				Class.forName(RuleServiceProviderImpl.class.getName());

				final RuleServiceProviderImpl serviceProvider = (RuleServiceProviderImpl) RuleServiceProviderManager
						.getRuleServiceProvider(RuleServiceProviderImpl.RULE_SERVICE_PROVIDER);

				final RuleAdministrator administrator = serviceProvider
						.getRuleAdministrator();

				final LocalRuleExecutionSetProvider executionSetProvider = administrator
						.getLocalRuleExecutionSetProvider(null);

				final XQParameters params = servCtx.getParameters();

				final String mode = params.getParameter(PARAM_NAME_MODE,
						XQConstants.PARAM_STRING);

				RuleExecutionSet executionSet = null;

				if (mode.equals(MODE_FILE)) {
					final String drlFile = params.getParameter(
							PARAM_NAME_DRL_FILE, XQConstants.PARAM_STRING);

					final Map executionSetProps = new HashMap();

					executionSetProps.put(Constants.RES_SOURCE, "drl");

					final String dslFile = params.getParameter(
							PARAM_NAME_DSL_FILE, XQConstants.PARAM_STRING);

					if (dslFile != null)
						executionSetProps.put(Constants.RES_DSL,
								new StringReader(dslFile));

					executionSet = executionSetProvider.createRuleExecutionSet(
							new StringReader(drlFile), executionSetProps);
				} else if (mode.equals(MODE_REPOSITORY)) {
					final RuleExecutionSetRepository repository = serviceProvider
							.getRepository();

					final String uri = params.getParameter(PARAM_NAME_URI,
							XQConstants.PARAM_STRING);

					executionSet = repository.getRuleExecutionSet(uri);
				}

				administrator.registerRuleExecutionSet(executionSet.getName(),
						executionSet, null);

				/* Execute the rules */
				final RuleRuntime runtime = serviceProvider.getRuleRuntime();

				final StatefulRuleSession session = (StatefulRuleSession) runtime
						.createRuleSession(executionSet.getName(), null,
								RuleRuntime.STATEFUL_SESSION_TYPE);

				final XQEnvelope origEnv = servCtx.getNextIncoming();

				/* Push the original envelope to the rule engine */
				final Handle handle = session.addObject(origEnv);

				session.executeRules();

				/* Pull the new envelope from the rule engine */
				final XQEnvelope newEnv = (XQEnvelope) session
						.getObject(handle);

				session.release();

				final Iterator addressIterator = newEnv.getAddresses();

				if (addressIterator.hasNext())
					servCtx.addOutgoing(newEnv);

			}

		} catch (final Exception e) {
			throw new XQServiceException(e);
		}

	}

}