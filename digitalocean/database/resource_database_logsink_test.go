package database_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/digitalocean/godo"
	"github.com/digitalocean/terraform-provider-digitalocean/digitalocean/acceptance"
	"github.com/digitalocean/terraform-provider-digitalocean/digitalocean/config"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccDigitalOceanDatabaseLogsink_Basic(t *testing.T) {
	var databaseLogsink godo.DatabaseLogsink
	databaseClusterName := acceptance.RandomTestName()
	databaseLogsinkName := acceptance.RandomTestName()
	databaseLogsinkType := "rsyslog"
	databaseLogsinkNameUpdated := databaseLogsinkName + "-up"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:          func() { acceptance.TestAccPreCheck(t) },
		ProviderFactories: acceptance.TestAccProviderFactories,
		CheckDestroy:      testAccCheckDigitalOceanDatabaseLogsinkDestroy,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testAccCheckDigitalOceanDatabaseLogsinkConfigRsyslog, databaseClusterName, databaseLogsinkName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckDigitalOceanDatabaseLogsinkExists("digitalocean_database_log_sink.foobar_log_sink", &databaseLogsink),
					testAccCheckDigitalOceanDatabaseLogsinkAttributes(&databaseLogsink, databaseLogsinkName, databaseLogsinkType),
					resource.TestCheckResourceAttrSet(
						"digitalocean_database_log_sink.foobar_log_sink", "id"),
					resource.TestCheckResourceAttr(
						"digitalocean_database_log_sink.foobar_log_sink", "name", databaseLogsinkName),
					resource.TestCheckResourceAttr(
						"digitalocean_database_log_sink.foobar_log_sink", "type", databaseLogsinkType),
					resource.TestCheckResourceAttrSet(
						"digitalocean_database_log_sink.foobar_log_sink", "config"),
				),
			},
			{
				Config: fmt.Sprintf(testAccCheckDigitalOceanDatabaseLogsinkConfigRsyslog, databaseClusterName, databaseLogsinkNameUpdated),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckDigitalOceanDatabaseLogsinkExists("digitalocean_database_log_sink.foobar_log_sink", &databaseLogsink),
					testAccCheckDigitalOceanDatabaseLogsinkNotExists("digitalocean_database_log_sink.foobar_log_sink", databaseLogsinkName),
					testAccCheckDigitalOceanDatabaseLogsinkAttributes(&databaseLogsink, databaseLogsinkNameUpdated, databaseLogsinkType),
					resource.TestCheckResourceAttr(
						"digitalocean_database_log_sink.foobar_log_sink", "name", databaseLogsinkNameUpdated),
				),
			},
		},
	})
}

func testAccCheckDigitalOceanDatabaseLogsinkDestroy(s *terraform.State) error {
	client := acceptance.TestAccProvider.Meta().(*config.CombinedConfig).GodoClient()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "digitalocean_database_log_sink" {
			continue
		}
		id := rs.Primary.Attributes["id"]
		clusterID := rs.Primary.Attributes["cluster_id"]

		// Try to find the logsink
		_, _, err := client.Databases.GetLogsink(context.Background(), clusterID, id)

		if err == nil {
			return fmt.Errorf("Database Logsink still exists")
		}
	}

	return nil
}

func testAccCheckDigitalOceanDatabaseLogsinkExists(n string, databaseLogsink *godo.DatabaseLogsink) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]

		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No Database Logsink ID is set")
		}

		client := acceptance.TestAccProvider.Meta().(*config.CombinedConfig).GodoClient()
		clusterID := rs.Primary.Attributes["cluster_id"]
		name := rs.Primary.Attributes["name"]

		foundDatabaseLogsink, _, err := client.Databases.GetLogsink(context.Background(), clusterID, name)

		if err != nil {
			return err
		}

		if foundDatabaseLogsink.Name != name {
			return fmt.Errorf("Database logsink not found")
		}

		*databaseLogsink = *foundDatabaseLogsink

		return nil
	}
}

func testAccCheckDigitalOceanDatabaseLogsinkNotExists(n string, id string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]

		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No Database Logsink ID is set")
		}

		client := acceptance.TestAccProvider.Meta().(*config.CombinedConfig).GodoClient()
		clusterID := rs.Primary.Attributes["cluster_id"]

		_, resp, err := client.Databases.GetDB(context.Background(), clusterID, id)

		if err != nil && resp.StatusCode != http.StatusNotFound {
			return err
		}

		if err == nil {
			return fmt.Errorf("Database Logsink %s still exists", id)
		}

		return nil
	}
}

func testAccCheckDigitalOceanDatabaseLogsinkAttributes(databaseLogsink *godo.DatabaseLogsink, name string, sink_type string) resource.TestCheckFunc {
	return func(s *terraform.State) error {

		if databaseLogsink.Name != name {
			return fmt.Errorf("Bad name: %s", databaseLogsink.Name)
		}

		if databaseLogsink.Type != sink_type {
			return fmt.Errorf("Bad type: %s", databaseLogsink.Type)
		}

		return nil
	}
}

const testAccCheckDigitalOceanDatabaseLogsinkConfigRsyslog = `
resource "digitalocean_database_cluster" "foobar" {
  name       = "%s"
  engine     = "pg"
  version    = "15"
  size       = "db-s-1vcpu-1gb"
  region     = "nyc1"
  node_count = 1

  maintenance_window {
    day  = "friday"
    hour = "13:00:00"
  }
}

resource "digitalocean_database_log_sink" "foobar_log_sink" {
  cluster_id = digitalocean_database_cluster.foobar.id
  name = "%s"
  type = "rsyslog"

  config {
    server "localhost"
	port   443
	tls    true
	format "rfc5424"
  }
}`
