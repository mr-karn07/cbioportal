package org.cbioportal.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.cbioportal.model.CancerStudy;
import org.cbioportal.model.CancerStudyTags;
import org.cbioportal.model.TypeOfCancer;
import org.cbioportal.model.meta.BaseMeta;
import org.cbioportal.persistence.StudyRepository;
import org.cbioportal.service.CancerTypeService;
import org.cbioportal.service.ReadPermissionService;
import org.cbioportal.service.StudyService;
import org.cbioportal.service.exception.StudyNotFoundException;
import org.cbioportal.utils.security.AccessLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

@Service
public class StudyServiceImpl implements StudyService {
    @Value("${extnserver.service.url:}")
    private String extnServerServiceURL;

    @Autowired
    private StudyRepository studyRepository;
    
    @Autowired
    private CancerTypeService cancerTypeService;

    @Autowired
    private ReadPermissionService readPermissionService;

    @Override
    @PostFilter("hasPermission(filterObject,#accessLevel)")
    public List<CancerStudy> getAllStudies(String keyword, String projection, Integer pageSize, Integer pageNumber,
                                           String sortBy, String direction, Authentication authentication, AccessLevel accessLevel) {

        List<CancerStudy> allStudies = studyRepository.getAllStudies(keyword, projection, pageSize, pageNumber, sortBy, direction);
        Map<String,CancerStudy> sortedAllStudiesByCancerStudyIdentifier = allStudies.stream().collect(Collectors.toMap(c -> c.getCancerStudyIdentifier(), c -> c, (e1, e2) -> e2, LinkedHashMap::new));
        if (keyword != null && (pageSize == null || allStudies.size() < pageSize)) {
            List<CancerStudy> primarySiteMatchingStudies = findPrimarySiteMatchingStudies(keyword);
            for (CancerStudy cancerStudy : primarySiteMatchingStudies) {
                if (!sortedAllStudiesByCancerStudyIdentifier.containsKey(cancerStudy.getCancerStudyIdentifier())) {
                    sortedAllStudiesByCancerStudyIdentifier.put(cancerStudy.getCancerStudyIdentifier(), cancerStudy);
                }
                if (pageSize != null && sortedAllStudiesByCancerStudyIdentifier.size() == pageSize) {
                    break;
                }
            }
        }

        // For authenticated portals it is essential to make a new list, such
        // that @PostFilter does not taint the list stored in the mybatis
        // second-level cache. When making changes to this make sure to copy the
        // allStudies list at least for the AUTHENTICATE.equals("true") case
        List<CancerStudy> returnedStudyObjects = sortedAllStudiesByCancerStudyIdentifier.values().stream().collect(Collectors.toList());
        
        // When using prop. 'skin.home_page.show_unauthorized_studies' this endpoint
        // returns the full list of studies, some of which can be accessed by the user.
        readPermissionService.setReadPermission(returnedStudyObjects, authentication);
        
        return returnedStudyObjects;
    }

    @Override
    public BaseMeta getMetaStudies(String keyword) {
        if (keyword == null) {
            return studyRepository.getMetaStudies(keyword);
        }
        else {
            BaseMeta baseMeta = new BaseMeta();
            baseMeta.setTotalCount(getAllStudies(keyword, "SUMMARY", null, null, null, null, null, AccessLevel.READ).size());
            return baseMeta;
        }
    }

    @Override
    public CancerStudy getStudy(String studyId) throws StudyNotFoundException {

        CancerStudy cancerStudy = studyRepository.getStudy(studyId, "DETAILED");
        if (cancerStudy == null) {
            throw new StudyNotFoundException(studyId);
        }

        return cancerStudy;
    }

    @Override
	public List<CancerStudy> fetchStudies(List<String> studyIds, String projection) {
        
        return studyRepository.fetchStudies(studyIds, projection);
	}

    @Override
	public BaseMeta fetchMetaStudies(List<String> studyIds) {
        
        return studyRepository.fetchMetaStudies(studyIds);
	}
    
    @Override
    @PreAuthorize("hasPermission(#studyId, 'CancerStudyId', #accessLevel)")
    public CancerStudyTags getTags(String studyId, AccessLevel accessLevel) {

        return studyRepository.getTags(studyId);
    }

    @Override
    public List<CancerStudyTags> getTagsForMultipleStudies(List<String> studyIds) {

        return studyRepository.getTagsForMultipleStudies(studyIds);
    }

    private List<CancerStudy> findPrimarySiteMatchingStudies(String keyword) {

        List<CancerStudy> matchingStudies = new ArrayList<>();

        List<String> matchingCancerTypes = new ArrayList<>();
        for (Map.Entry<String, TypeOfCancer> entry : cancerTypeService.getPrimarySiteMap().entrySet()) {
            if (entry.getValue().getTypeOfCancerId().toLowerCase().contains(keyword.toLowerCase()) || 
                entry.getValue().getName().toLowerCase().contains(keyword.toLowerCase())) {
                matchingCancerTypes.add(entry.getKey());
            }
        }
        if (!matchingCancerTypes.isEmpty()) {
            List<CancerStudy> allUnfilteredStudies = studyRepository.getAllStudies(null, "SUMMARY", null, null, null, null);
            for (CancerStudy cancerStudy : allUnfilteredStudies) {
                if (matchingCancerTypes.contains(cancerStudy.getTypeOfCancerId())) {
                    matchingStudies.add(cancerStudy);
                }
            }
        }
        return matchingStudies;
    }

    @Override
    public String processFile(MultipartFile file) {  
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        String response;
        RestTemplate restTemplate = new RestTemplate();

        try {
            if (!file.isEmpty()) {
                map.add("file", new MultipartInputStreamFileResource(file.getInputStream(), file.getOriginalFilename()));
            }
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);

            String url = extnServerServiceURL;

            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
            ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);

            // Check the status code
            if (responseEntity.getStatusCode() == HttpStatus.OK) {
                response = "File uploaded successfully";
            } else {
                response = "File upload failed: " + responseEntity.getStatusCode();
            }

        } catch (HttpStatusCodeException e) {
            response = e.getResponseBodyAsString();
        } catch (Exception e) {
            response = e.getMessage();
        }

        return response;
    }
}

class MultipartInputStreamFileResource extends InputStreamResource {

    private final String filename;

    MultipartInputStreamFileResource(InputStream inputStream, String filename) {
        super(inputStream);
        this.filename = filename;
    }

    @Override
    public String getFilename() {
        return this.filename;
    }

    @Override
    public long contentLength() throws IOException {
        return -1; // we do not want to generally read the whole stream into memory ...
    }
}

