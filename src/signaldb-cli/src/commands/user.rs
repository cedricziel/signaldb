use clap::{Subcommand, ValueEnum};
use signaldb_sdk::Client;
use signaldb_sdk::types::CreateUserRequest;

#[derive(Clone, Copy, ValueEnum)]
pub enum RoleArg {
    Admin,
    Member,
    Viewer,
}

impl RoleArg {
    fn as_str(self) -> &'static str {
        match self {
            RoleArg::Admin => "admin",
            RoleArg::Member => "member",
            RoleArg::Viewer => "viewer",
        }
    }
}

#[derive(Subcommand)]
pub enum UserAction {
    /// Create a human user and grant an initial tenant membership
    Create {
        /// Login email address
        email: String,
        /// Display name
        #[arg(long)]
        display_name: Option<String>,
        /// Initial tenant membership
        #[arg(long)]
        tenant: String,
        /// Initial tenant role
        #[arg(long, value_enum, default_value = "admin")]
        role: RoleArg,
        /// Grant instance-administrator status
        #[arg(long)]
        instance_admin: bool,
        /// Password; prefer SIGNALDB_USER_PASSWORD to avoid shell history
        #[arg(long, env = "SIGNALDB_USER_PASSWORD", hide_env_values = true)]
        password: String,
    },
}

impl UserAction {
    pub async fn run(self, client: &Client) -> anyhow::Result<()> {
        match self {
            UserAction::Create {
                email,
                display_name,
                tenant,
                role,
                instance_admin,
                password,
            } => {
                if password.len() < 12 {
                    anyhow::bail!("password must be at least 12 characters");
                }
                let user = client
                    .create_user()
                    .body(CreateUserRequest {
                        email,
                        display_name,
                        password,
                        instance_admin: Some(instance_admin),
                        tenant: tenant.clone(),
                        role: Some(role.as_str().to_string()),
                    })
                    .send()
                    .await
                    .map_err(|e| anyhow::Error::new(e).context("create user failed"))?
                    .into_inner();
                println!(
                    "Created user '{}' ({}) in tenant '{tenant}'.",
                    user.email, user.id
                );
            }
        }
        Ok(())
    }
}
