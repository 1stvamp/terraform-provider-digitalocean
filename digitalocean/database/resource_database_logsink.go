package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/digitalocean/godo"
	"github.com/digitalocean/terraform-provider-digitalocean/digitalocean/config"
	"github.com/digitalocean/terraform-provider-digitalocean/internal/mutexkv"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

var logsinkMutexKV = mutexkv.NewMutexKV()

func ResourceDigitalOceanDatabaseLogsink() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceDigitalOceanDatabaseLogsinkCreate,
		ReadContext:   resourceDigitalOceanDatabaseLogsinkRead,
		UpdateContext: resourceDigitalOceanDatabaseLogsinkUpdate,
		DeleteContext: resourceDigitalOceanDatabaseLogsinkDelete,
		Importer: &schema.ResourceImporter{
			State: resourceDigitalOceanDatabaseLogsinkImport,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.NoZeroValues,
			},
			"cluster_id": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.NoZeroValues,
			},
			"type": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.NoZeroValues,
			},
			"config": {
				Type:     schema.TypeMap,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"url": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"index_prefix": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"index_days_max": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"timeout": {
							Type:     schema.TypeFloat,
							Optional: true,
						},
						"server": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"port": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"tls": {
							Type:     schema.TypeBool,
							Optional: true,
						},
						"format": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"logline": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"sd": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"ca": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"key": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"cert": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

func resourceDigitalOceanDatabaseLogsinkCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	client := meta.(*config.CombinedConfig).GodoClient()
	clusterID := d.Get("cluster_id").(string)

	opts := &godo.DatabaseCreateLogsinkRequest{
		Name:   d.Get("name").(string),
		Type:   d.Get("type").(string),
		Config: d.Get("config").(*godo.DatabaseLogsinkConfig),
	}

	// Prevent parallel creation of log sinks for same cluster.
	key := fmt.Sprintf("digitalocean_database_cluster/%s/log_sinks", clusterID)
	logsinkMutexKV.Lock(key)
	defer logsinkMutexKV.Unlock(key)

	log.Printf("[DEBUG] Database Logsink create configuration: %#v", opts)
	logsink, _, err := client.Databases.CreateLogsink(context.Background(), clusterID, opts)
	if err != nil {
		return diag.Errorf("Error creating Database Logsink: %s", err)
	}

	d.SetId(makeDatabaseLogsinkID(clusterID, logsink.Name))
	log.Printf("[INFO] Database Logsink Name: %s", logsink.Name)

	setDatabaseLogsinkAttributes(d, logsink)

	return nil
}

func resourceDigitalOceanDatabaseLogsinkRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	client := meta.(*config.CombinedConfig).GodoClient()
	id := d.Get("id").(string)
	clusterID := d.Get("cluster_id").(string)

	// Check if the database log sink still exists
	logsink, resp, err := client.Databases.GetLogsink(context.Background(), clusterID, id)
	if err != nil {
		// If the database log sink is somehow already destroyed, mark as
		// successfully gone
		if resp != nil && resp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return diag.Errorf("Error retrieving Database Logsink: %s", err)
	}

	setDatabaseLogsinkAttributes(d, logsink)

	return nil
}

func setDatabaseLogsinkAttributes(d *schema.ResourceData, logsink *godo.DatabaseLogsink) diag.Diagnostics {
	d.Set("id", logsink.ID)
	d.Set("name", logsink.Name)
	d.Set("type", logsink.Type)

	if _, ok := d.GetOk("config"); ok {
		if err := d.Set("config", flattenLogsinkConfig(logsink.Config)); err != nil {
			return diag.Errorf("[DEBUG] Error setting longsink config - error: %#v", err)
		}
	}

	return nil
}

func resourceDigitalOceanDatabaseLogsinkUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	client := meta.(*config.CombinedConfig).GodoClient()
	clusterID := d.Get("cluster_id").(string)
	name := d.Get("name").(string)

	opts := &godo.DatabaseUpdateLogsinkRequest{
		Config: expandLogsinkConfig(d.Get("config").([]interface{})),
	}

	// Prevent parallel creation of log sinks for same cluster.
	key := fmt.Sprintf("digitalocean_database_cluster/%s/log_sinks", clusterID)
	logsinkMutexKV.Lock(key)
	defer logsinkMutexKV.Unlock(key)

	log.Printf("[DEBUG] Database Logsink update configuration: %#v", opts)
	resp, err := client.Databases.UpdateLogsink(context.Background(), clusterID, name, opts)
	if err != nil {
		return diag.Errorf("Error updating Database Logsink: %s", err)
	}

	// As of 1.125.0 of godo Databases.UpdateLogsink does not return the
	// mutated godo.DatabaseLogsink instance, so we need to recreate it
	// from the response
	logsink := new(godo.DatabaseLogsink)
	json.NewDecoder(resp.Body).Decode(logsink)
	setDatabaseLogsinkAttributes(d, logsink)

	return nil
}

func resourceDigitalOceanDatabaseLogsinkDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	client := meta.(*config.CombinedConfig).GodoClient()
	clusterID := d.Get("cluster_id").(string)
	id := d.Get("id").(string)

	// Prevent parallel deletion of log sinks for same cluster.
	key := fmt.Sprintf("digitalocean_database_cluster/%s/log_sinks", clusterID)
	logsinkMutexKV.Lock(key)
	defer logsinkMutexKV.Unlock(key)

	log.Printf("[INFO] Deleting Database Logsink: %s", id)
	_, err := client.Databases.DeleteLogsink(context.Background(), clusterID, id)
	if err != nil {
		return diag.Errorf("Error deleting Database Logsink: %s", err)
	}

	d.SetId("")
	return nil
}

func resourceDigitalOceanDatabaseLogsinkImport(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	if strings.Contains(d.Id(), ",") {
		s := strings.Split(d.Id(), ",")
		d.SetId(makeDatabaseLogsinkID(s[0], s[1]))
		d.Set("cluster_id", s[0])
		d.Set("name", s[1])
	} else {
		return nil, errors.New("must use the ID of the source database cluster and the name of the logsink joined with a comma (e.g. `id,name`)")
	}

	return []*schema.ResourceData{d}, nil
}

func expandLogsinkConfig(config []interface{}) *godo.DatabaseLogsinkConfig {
	configMap := config[0].(map[string]interface{})

	sinkConfig := &godo.DatabaseLogsinkConfig{
		URL:          configMap["url"].(string),
		IndexPrefix:  configMap["index_prefix"].(string),
		IndexDaysMax: configMap["index_days_max"].(int),
		Timeout:      configMap["timeout"].(float32),
		Server:       configMap["server"].(string),
		Port:         configMap["port"].(int),
		TLS:          configMap["tls"].(bool),
		Format:       configMap["format"].(string),
		Logline:      configMap["logline"].(string),
		SD:           configMap["sd"].(string),
		CA:           configMap["ca"].(string),
		Key:          configMap["key"].(string),
		Cert:         configMap["cert"].(string),
	}

	return sinkConfig
}

func flattenLogsinkConfig(config *godo.DatabaseLogsinkConfig) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	item := make(map[string]interface{})

	item["urls"] = config.URL
	item["index_prefix"] = config.IndexPrefix
	item["index_days_max"] = config.IndexDaysMax
	item["timeout"] = config.Timeout
	item["server"] = config.Server
	item["port"] = config.Port
	item["tls"] = config.TLS
	item["format"] = config.Format
	item["logline"] = config.Logline
	item["sd"] = config.SD
	item["ca"] = config.CA
	item["key"] = config.Key
	item["cert"] = config.Cert

	result = append(result, item)

	return result
}

func makeDatabaseLogsinkID(clusterID string, name string) string {
	return fmt.Sprintf("%s/logsink/%s", clusterID, name)
}
