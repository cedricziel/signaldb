use clap::{Subcommand, ValueEnum};
use common::catalog::{Catalog, MembershipRole};
use common::config::Configuration;

#[derive(Clone, Copy, ValueEnum)]
pub enum RoleArg {
    Admin,
    Member,
    Viewer,
}

impl From<RoleArg> for MembershipRole {
    fn from(value: RoleArg) -> Self {
        match value {
            RoleArg::Admin => MembershipRole::Admin,
            RoleArg::Member => MembershipRole::Member,
            RoleArg::Viewer => MembershipRole::Viewer,
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
    pub async fn run(self, config: &Configuration) -> anyhow::Result<()> {
        let dsn = config
            .discovery
            .as_ref()
            .map(|discovery| discovery.dsn.as_str())
            .unwrap_or(config.database.dsn.as_str());
        let catalog = Catalog::new(dsn).await?;

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
                if catalog.get_tenant(&tenant).await?.is_none() {
                    anyhow::bail!(
                        "tenant '{tenant}' is not present in the service catalog; start SignalDB once to sync configured tenants"
                    );
                }
                let password_hash =
                    tokio::task::spawn_blocking(move || common::auth::hash_password(&password))
                        .await
                        .map_err(|error| {
                            anyhow::anyhow!("password hashing task failed: {error}")
                        })??;
                let user = catalog
                    .create_user(
                        &email,
                        display_name.as_deref(),
                        &password_hash,
                        instance_admin,
                    )
                    .await?;
                catalog
                    .upsert_tenant_membership(&user.id, &tenant, role.into())
                    .await?;
                println!(
                    "Created user '{}' ({}) in tenant '{tenant}'.",
                    user.email, user.id
                );
            }
        }
        Ok(())
    }
}
