select 
	coalesce(appellation, 'inconnu') as appellation,
	coalesce(auteur, 'inconnu') as auteur,
	coalesce(date_creation, 'inconnu') as date_creation,
	coalesce(region, 'inconnu') as region, 
	coalesce(departement, 'inconnu') as departement, 
	LEFT(description, 300) as "description"
from staging.joconde

