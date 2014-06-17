package org.ektorp.http.clientconfig;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.ThrowsException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class CredentialsProviderConfigurerTest {

	@Test
	public void shouldConfigureWhenUsernameAndPasswordAreBothNotEmpty() {
		CredentialsProvider credentialProvider = mock(CredentialsProvider.class, new ThrowsException(new UnsupportedOperationException()));
		doNothing().when(credentialProvider).setCredentials(any(AuthScope.class), any(Credentials.class));
		doCallRealMethod().when(credentialProvider).toString();
		CredentialsProviderConfigurer tested = new CredentialsProviderConfigurer();
		tested.setUsername("non empty username");
		tested.setPassword("non empty password");
		tested.configure(credentialProvider);
	}

	@Test
	public void shouldNotConfigureWhenUsernameIsNull() {
		CredentialsProvider credentialProvider = mock(CredentialsProvider.class, new ThrowsException(new UnsupportedOperationException()));
		CredentialsProviderConfigurer tested = new CredentialsProviderConfigurer();
		tested.setUsername(null);
		tested.setPassword("non empty password");
		tested.configure(credentialProvider);
	}

	@Test
	public void shouldNotConfigureWhenUsernameIsEmpty() {
		CredentialsProvider credentialProvider = mock(CredentialsProvider.class, new ThrowsException(new UnsupportedOperationException()));
		CredentialsProviderConfigurer tested = new CredentialsProviderConfigurer();
		tested.setUsername(""); // username is an empty String
		tested.setPassword("non empty password");
		tested.configure(credentialProvider);
	}

	@Test
	public void shouldNotConfigureWhenPasswordIsNull() {
		CredentialsProvider credentialProvider = mock(CredentialsProvider.class, new ThrowsException(new UnsupportedOperationException()));
		CredentialsProviderConfigurer tested = new CredentialsProviderConfigurer();
		tested.setUsername("non empty username");
		tested.setPassword(null);
		tested.configure(credentialProvider);
	}


}
